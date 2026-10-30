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
import types
import unittest
from unittest import mock

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
        patch = mock.patch.object(lyse.routines, 'start_worker', start_worker)
        patch.start()
        self.addCleanup(patch.stop)
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
        self.call('add_routines', 'singleshot', [a, b])
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
        self.call('remove_shots', ['1.h5'])
        self.assertEqual(shots.filepaths, ['0.h5'])
        with self.assertRaisesRegex(ValueError, '2.h5'):
            self.call('rerun_shots', ['2.h5'])


if __name__ == '__main__':
    unittest.main()
