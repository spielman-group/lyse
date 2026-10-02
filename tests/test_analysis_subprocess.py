#####################################################################
#                                                                   #
# /tests/test_analysis_subprocess.py                                #
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
"""The analysis worker: a plot window's theme change and named figure, and a GUI routine's
worker, which runs the routine folder as a real process."""
import sys
import tempfile
import unittest
import warnings
from pathlib import Path
from unittest import mock

from qtutils.qt import QtCore, QtGui, QtWidgets

with warnings.catch_warnings():
    warnings.simplefilter('ignore')
    import lyse.analysis_subprocess
    from labscript_utils.ls_zprocess import ProcessTree
    from lyse.routine import RoutineSettings
    from lyse.utils import LYSE_DIR


class Child(QtWidgets.QWidget):
    """Counts the repaints the window gives it."""
    repaints = 0

    def setStyleSheet(self, sheet):
        self.repaints += 1
        super().setStyleSheet(sheet)


def change_theme(qapplication):
    """Switch the application palette, as a light/dark switch does, and return
    what escaped to sys.excepthook."""
    seen = []
    original_hook, original_palette = sys.excepthook, qapplication.palette()
    sys.excepthook = lambda cls, exc, tb: seen.append(exc)
    try:
        palette = QtGui.QPalette(original_palette)
        palette.setColor(QtGui.QPalette.ColorRole.Window, QtGui.QColor('#202020'))
        qapplication.setPalette(palette)
        qapplication.processEvents()
    finally:
        sys.excepthook = original_hook
        qapplication.setPalette(original_palette)
    return seen


class PlotWindowTests(unittest.TestCase):

    def test_a_theme_change_repaints_the_window_without_error(self):
        qapplication = QtWidgets.QApplication.instance() or QtWidgets.QApplication(['test'])
        window = lyse.analysis_subprocess.PlotWindow(
            None, analysis_filepath='routine.py', analysis_identifier=0)
        child = Child(window)
        self.assertEqual(change_theme(qapplication), [])
        self.assertGreater(child.repaints, 0)


class NamedFigureTests(unittest.TestCase):

    def test_a_figure_named_by_a_string_gets_a_window_that_keeps_its_geometry(self):
        """A routine may name its figure, as plt.figure('Temperature') does."""
        qapplication = QtWidgets.QApplication.instance() or QtWidgets.QApplication(['test'])
        with tempfile.TemporaryDirectory() as folder, \
                mock.patch.object(lyse.analysis_subprocess, 'config_dir', folder):
            def a_window():
                return lyse.analysis_subprocess.PlotWindow(
                    None, analysis_filepath='routine.py', analysis_identifier='Temperature')
            window = a_window()
            window.resize(321, 234)
            window.save_geometry()
            self.assertEqual(a_window().size(), QtCore.QSize(321, 234))


# The lyse_routine.py of a routine whose run reports the box it was given and then changes the
# box, as a user may while a run is going, by way of helper.py beside it. Its slow.h5 shot then
# waits for the settings that a quit saves, and close() records the thread that called it.
ROUTINE = '''
import threading
import time
from pathlib import Path
import lyse
from labscript_utils.ls_zprocess import ProcessTree
from qtutils import inmain
from qtutils.qt import QtWidgets
from .helper import widen

HERE = Path(__file__).parent


class Analysis(lyse.Routine):
    def __init__(self):
        self.box = QtWidgets.QSpinBox(objectName='threshold', value=3)
        self.saved_widgets(self.box)

    def run(self, path, paths):
        if path.endswith('bad.h5'):
            raise RuntimeError('bad shot')
        run = lyse.Run(path)
        run.save_result('seen', self.values.threshold, save_to_h5=False)
        inmain(self.box.setValue, widen(self.values.threshold))
        run.save_result('kept', self.values.threshold, save_to_h5=False)
        if path.endswith('slow.h5'):
            ProcessTree.instance().event('running', role='post').post('slow')
            while 'threshold = 7' not in Path('<settings>').read_text():
                time.sleep(0.01)
            assert not (HERE / 'closed').exists()

    def close(self):
        with open(HERE / 'closed', 'a') as f:
            print(threading.current_thread().name, file=f)
'''
HELPER = 'def widen(value):\n    return value + 4\n'


class Worker:
    """A worker process, started and talked to as lyse does."""

    def __init__(self, routine):
        self.to_worker, self.from_worker, self.process = ProcessTree.instance().subprocess(
            str(LYSE_DIR / 'analysis_subprocess.py'), startup_timeout=30)
        self.to_worker.put(str(routine))

    def send(self, task, data=None):
        self.to_worker.put([task, data])

    def reply(self):
        return self.from_worker.get(timeout=60)

    def analyse(self, shot):
        self.send('analyse', (str(shot), None))
        return self.reply()

    def quit(self):
        self.send('quit')
        return self.process.wait(timeout=60)

    def kill(self):
        self.process.kill()
        self.process.wait()


class GuiWorkerTests(unittest.TestCase):

    def setUp(self):
        self.folder = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.routine = self.folder / 'routine.lyse'
        self.routine.mkdir()
        self.source = self.routine / 'lyse_routine.py'
        (self.routine / 'helper.py').write_text(HELPER)
        shots = [self.folder / f'{name}.h5' for name in ('good', 'bad', 'slow')]
        self.good, self.bad, self.slow = shots
        for shot in shots:
            shot.touch()
        self.settings = RoutineSettings(self.routine, lyse.analysis_subprocess.config_dir)
        self.addCleanup(self.settings.path.unlink, missing_ok=True)

    def start(self, source):
        self.source.write_text(source.replace('<settings>', str(self.settings.path)))
        worker = Worker(self.routine)
        self.addCleanup(worker.kill)
        return worker

    def test_shots_are_analysed_in_turn_with_the_controls_saved_first(self):
        worker = self.start(ROUTINE)
        for seen in (3, 7):
            self.assertEqual(worker.analyse(self.good), ['done', {str(self.good): {
                ('routine', 'seen'): seen, ('routine', 'kept'): seen}}])
            self.assertEqual(self.settings.load()[0], {'threshold': seen})

    def test_a_failed_run_replies_error_without_earlier_results(self):
        worker = self.start(ROUTINE)
        self.assertEqual(worker.analyse(self.good)[0], 'done')
        self.assertEqual(worker.analyse(self.bad), ['error', {}])
        # The analysis thread and the command listener have survived.
        self.assertEqual(worker.analyse(self.good)[0], 'done')

    def test_quit_waits_for_run_and_calls_close_once(self):
        running = ProcessTree.instance().event('running')
        worker = self.start(ROUTINE)
        worker.send('analyse', (str(self.slow), None))
        running.wait('slow', timeout=60)
        worker.send('quit')
        # The run returns once the quit has saved the box, and its snapshot is not replaced.
        self.assertEqual(worker.reply(), ['done', {str(self.slow): {
            ('routine', 'seen'): 3, ('routine', 'kept'): 3}}])
        self.assertEqual(worker.process.wait(timeout=60), 0)
        self.assertEqual((self.routine / 'closed').read_text().split(), ['MainThread'])

    def test_a_routine_that_cannot_load_fails_every_analysis_until_restart(self):
        for broken in ['import lyse\n'
                       'class A(lyse.Routine):\n    def __init__(self): raise ValueError\n',
                       'def (:\n']:
            worker = self.start(broken)
            self.assertEqual(worker.analyse(self.good), ['error', {}])
            # Fixed, but the worker keeps its failure until it is restarted.
            self.source.write_text(ROUTINE)
            self.assertEqual(worker.analyse(self.good), ['error', {}])
            self.assertEqual(worker.quit(), 0)
            self.assertEqual(self.start(ROUTINE).analyse(self.good)[0], 'done')

    def test_show_gets_no_reply_so_the_next_analysis_gets_its_own(self):
        classic = self.folder / 'classic.py'
        classic.write_text(
            'import lyse\nlyse.Run(lyse.path).save_result("seen", 7, save_to_h5=False)\n')
        classic_worker = Worker(classic)
        self.addCleanup(classic_worker.kill)
        cases = [(self.start(ROUTINE), {('routine', 'seen'): 3, ('routine', 'kept'): 3}),
                 (classic_worker, {('classic', 'seen'): 7})]
        for worker, results in cases:
            worker.send('show')
            self.assertEqual(worker.analyse(self.good), ['done', {str(self.good): results}])


if __name__ == '__main__':
    unittest.main()
