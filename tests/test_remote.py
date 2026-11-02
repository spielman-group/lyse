#####################################################################
#                                                                   #
# /tests/test_remote.py                                             #
#                                                                   #
# Copyright 2026, JQI                                               #
# Author: Ian Spielman                                              #
#                                                                   #
# This file is part of lyse, in the labscript suite                 #
# (see http://labscriptsuite.org), and is licensed under the        #
# Simplified BSD License. See the license.txt file in the root of   #
# the project for the full license.                                 #
#                                                                   #
#####################################################################
"""A client controls lyse's routine boxes and analysis through a real lyse server,
with the same result in the window as its buttons."""
import concurrent.futures
import os
import queue
import socket
import tempfile
import time
import types
import unittest
from pathlib import Path
from unittest import mock

import labscript_utils.h5_lock, h5py
from labscript_utils.ls_zprocess import ProcessTree
import pandas
from qtutils.qt import QtWidgets

import lyse.filebox
import lyse.routines
from lyse.client import LyseClient
from lyse.communication import LyseServer

qapplication = QtWidgets.QApplication.instance() or QtWidgets.QApplication(['test'])


def start_worker(process_tree, filepath, output_box_port, active):
    """A routine's worker, as lyse.routines.start_worker returns it, whose process
    exits when told to quit, in place of a real process."""
    process = types.SimpleNamespace(returncode=None, kill=lambda: None)
    process.poll = lambda: process.returncode
    to_worker = types.SimpleNamespace(put=lambda message: process.__dict__.update(returncode=0))
    return to_worker, queue.Queue(), process


class RemoteTests(unittest.TestCase):

    def setUp(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.scripts = [os.path.join(folder.name, name) for name in ['a.py', 'b.py']]
        for script in self.scripts:
            open(script, 'w').close()
        self.worker_patch = mock.patch.object(lyse.routines, 'start_worker', start_worker)
        self.worker_patch.start()
        self.addCleanup(self.worker_patch.stop)
        exp_config = types.SimpleNamespace(get=lambda *args: folder.name, getboolean=lambda *args: False)
        app = self.app = types.SimpleNamespace(
            process_tree=None, output_box=types.SimpleNamespace(output=lambda *args, **kwargs: None))
        queues = [queue.Queue() for _ in range(4)]
        app.singleshot_routinebox = lyse.routines.RoutineBox(
            app, QtWidgets.QVBoxLayout(), exp_config, None, queues[0], queues[1], None)
        app.multishot_routinebox = lyse.routines.RoutineBox(
            app, QtWidgets.QVBoxLayout(), exp_config, None, queues[2], queues[3], None, multishot=True)
        app.filebox = lyse.filebox.FileBox(app, QtWidgets.QVBoxLayout(), exp_config, *queues)
        with socket.socket() as probe:
            probe.bind(('127.0.0.1', 0))
            port = probe.getsockname()[1]
        server = LyseServer(app, port, bind_address='tcp://127.0.0.1')
        self.addCleanup(server.shutdown)
        self.client = LyseClient('localhost', port, 5)

    def call(self, command, *args):
        """The client's command, sent from a thread while this, the GUI thread, serves it."""
        with concurrent.futures.ThreadPoolExecutor(1) as pool:
            call = pool.submit(getattr(self.client, command), *args)
            while not call.done():
                qapplication.processEvents()
        return call.result()

    def test_a_client_sets_up_a_routine_box(self):
        a, b = self.scripts
        self.call('add_routines', 'singleshot', [os.path.relpath(a), b])
        self.call('set_routines_active', 'singleshot', [a], False)
        self.call('reorder_routines', 'singleshot', [b, a])
        self.assertEqual(self.call('get_routines', 'singleshot'), [
            {'filepath': b, 'active': True, 'status': None},
            {'filepath': a, 'active': False, 'status': None},
        ])
        box = self.app.singleshot_routinebox
        self.assertEqual([box.model.item(row, box.COL_NAME).text() for row in range(2)], ['b.py', 'a.py'])
        self.call('remove_routines', 'singleshot', [a])
        self.assertEqual([r['filepath'] for r in self.call('get_routines', 'singleshot')], [b])
        with self.assertRaisesRegex(ValueError, 'a.py'):
            self.call('remove_routines', 'singleshot', [a])
        with self.assertRaisesRegex(ValueError, 'doubleshot'):
            self.call('get_routines', 'doubleshot')

    def test_a_client_pauses_analysis_and_reruns_and_removes_shots(self):
        shots = self.app.filebox.shots_model
        shots.add_files(
            ['0.h5', '1.h5'], pandas.DataFrame({('filepath', ''): ['0.h5', '1.h5']}), ['remote'] * 2, done=True)
        self.call('set_analysis_paused', True)
        self.assertTrue(self.call('get_analysis_paused'))
        self.assertTrue(self.app.filebox.ui.pushButton_analysis_running.isChecked())
        self.call('rerun_shots', ['0.h5'])
        self.assertEqual(self.call('shot_status', ['0.h5', '1.h5']), {'0.h5': 'not done', '1.h5': 'done'})
        self.call('set_stop_on_failure', True)
        self.assertTrue(self.app.filebox.ui.checkBox_stop_on_failure.isChecked())
        self.call('remove_shots', ['1.h5'])
        self.assertEqual(shots.filepaths, ['0.h5'])
        with self.assertRaisesRegex(ValueError, '2.h5'):
            self.call('rerun_shots', ['2.h5'])

    def test_running_shots_cannot_be_removed_by_the_client_or_selection(self):
        shots = self.app.filebox.shots_model
        shots.add_files(['0.h5', '1.h5'], pandas.DataFrame({('filepath', ''): ['0.h5', '1.h5']}),
                        ['remote'] * 2)
        shots.start_attempt('0.h5')
        with self.assertRaisesRegex(ValueError, '0.h5'):
            self.call('remove_shots', ['0.h5', '1.h5'])
        shots.finish_analysis(['0.h5'])
        self.app.filebox.ui.tableView.selectRow(0)
        # Analysis starts while the confirmation dialog serves GUI events.
        self.app.question_dialog = lambda message: shots.start_attempt('0.h5')
        with mock.patch.object(self.app, 'error_dialog', create=True) as warning:
            shots.remove_selection()
            self.assertTrue(warning.called)
        self.assertEqual(shots.filepaths, ['0.h5', '1.h5'])
        self.call('remove_shots', ['1.h5'])
        self.assertFalse(shots.start_attempt('1.h5'))
        shots.finish_analysis(['0.h5'])
        self.call('remove_shots', ['0.h5'])
        self.assertEqual(shots.filepaths, [])

    def test_abort_stops_the_shot_even_when_the_worker_returns_success(self):
        self.worker_patch.stop()
        self.app.process_tree = ProcessTree.instance()
        self.client = LyseClient('localhost', self.client.port, 60)
        first, later = map(Path, self.scripts)
        first.write_text(
            'import lyse\nfrom pathlib import Path\nfrom qtutils.qt import QtCore\n'
            'lyse.delay_results_return()\n'
            # The timer runs only after the script has returned to the GUI event loop.
            'QtCore.QTimer.singleShot(0, lambda: Path(__file__).with_suffix(".ready").touch())\n')
        later.write_text('from pathlib import Path\nPath(__file__).with_suffix(".ran").touch()\n')
        self.call('add_routines', 'singleshot', self.scripts)
        for routine in self.app.singleshot_routinebox.routines:
            self.addCleanup(routine.worker.wait, timeout=10)
            self.addCleanup(routine.worker.kill)
        shot = first.with_suffix('.h5')
        with h5py.File(shot, 'w') as f:
            f.create_group('globals')
            f.attrs['sequence_id'] = '20261101T120000_abort'
            f.attrs['run time'] = '20261101T120001'
        self.call('add_shot', str(shot))
        deadline = time.monotonic() + 30
        while not first.with_suffix('.ready').exists():
            self.assertLess(time.monotonic(), deadline)
            self.call('get_analysis_paused')
            time.sleep(.01)
        self.call('abort')
        while self.call('shot_status', [str(shot)])[str(shot)] in ('not done', 'awaiting multishot'):
            self.assertLess(time.monotonic(), deadline)
        self.assertEqual(self.call('shot_status', [str(shot)]), {str(shot): 'aborted'})
        self.assertEqual(self.call('get_dataframe')['lyse'].iloc[0].dropna().to_dict(),
                         {'a': 'aborted', 'b': 'not run'})
        with h5py.File(shot, 'r') as f:
            self.assertNotIn('results', f)
        self.assertFalse(later.with_suffix('.ran').exists())


if __name__ == '__main__':
    unittest.main()
