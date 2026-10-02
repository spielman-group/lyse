#####################################################################
#                                                                   #
# /tests/test_routine.py                                            #
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
"""Tests of lyse.Routine and the pieces the worker builds one from."""
import contextlib
import importlib
import io
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import matplotlib.pyplot as plt
from qtutils.qt import QT_ENV, QtCore, QtWidgets

from labscript_utils.ls_zprocess import get_config
from labscript_utils.qtwidgets.outputbox import OutputBox
import lyse
from lyse.routine import (
    RoutineSettings, RoutineWindow, construct, read_saved_widgets, restore_layout, route_output,
    routine_class, routine_mode, save_layout)
from zprocess.process_tree import OutputInterceptor

QtTest = importlib.import_module(f'{QT_ENV}.QtTest')

# The tests' one application, held for the whole run: a destroyed application takes qtutils'
# inmain with it, and an OutputBox shows its text through that.
qapplication = QtWidgets.QApplication.instance() or QtWidgets.QApplication(['test'])


def namespace_of(source, **imports):
    namespace = {'__name__': 'routine_file', **imports}
    exec(source, namespace)
    return namespace


class ModeTests(unittest.TestCase):

    def test_the_mode_is_read_from_one_literal_declaration(self):
        gui = '"""A routine."""\nLYSE_MODE = "gui"\n'
        self.assertEqual(routine_mode(gui, 'routine.py'), 'gui')
        self.assertIsNone(routine_mode('import lyse\n', 'routine.py'))

    def test_a_file_without_a_readable_mode_is_an_error(self):
        for source, error in [
                ('LYSE_MODE = mode', ValueError),
                ('from base import LYSE_MODE', ValueError),
                ('LYSE_MODE = "gui"\nLYSE_MODE = "gui"', ValueError),
                ('if True:\n    LYSE_MODE = "gui"', ValueError),
                ('LYSE_MODE = "classic"', ValueError),
                ('LYSE_MODE = "gui"\ndef f(:', SyntaxError)]:
            with self.subTest(source=source), self.assertRaises(error):
                routine_mode(source, 'routine.py')


class DiscoveryTests(unittest.TestCase):

    def test_the_one_routine_class_is_found_whatever_its_names(self):
        class Imported(lyse.Routine):
            pass

        namespace = namespace_of('from lyse import Routine\n'
                                 'class Analysis(Routine): pass\n'
                                 'Alias = Analysis\n', Imported=Imported)
        self.assertIs(routine_class(namespace), namespace['Analysis'])

    def test_no_class_or_two_classes_is_an_error(self):
        with self.assertRaises(ValueError):
            routine_class(namespace_of('from lyse import Routine'))
        with self.assertRaisesRegex(ValueError, 'shared base class belongs in an imported module'):
            routine_class(namespace_of('from lyse import Routine\n'
                                       'class Base(Routine): pass\n'
                                       'class Analysis(Base): pass\n'))


class SavedWidgetsTests(unittest.TestCase):

    def test_a_saved_widget_takes_its_saved_value_and_reaches_the_snapshot(self):
        class Analysis(lyse.Routine):
            def __init__(self):
                self.box = QtWidgets.QSpinBox(objectName='threshold', value=5)
                self.saved_widgets(self.box)

        # A saved 0 is falsy, and is still restored over the box's default of 5.
        routine = construct(Analysis, {'threshold': 0}, RoutineWindow(), 'routine.py', None)
        self.assertEqual(routine.box.value(), 0)
        self.assertEqual(len(routine.values), 0)

        # A saved value the box rejects is reported and ignored.
        with contextlib.redirect_stderr(io.StringIO()) as report:
            rejected = construct(
                Analysis, {'threshold': 'text'}, RoutineWindow(), 'routine.py', None)
        self.assertEqual(rejected.box.value(), 5)
        self.assertIn('threshold', report.getvalue())

        routine.values, controls = read_saved_widgets(routine)
        self.assertEqual(routine.values.threshold, 0)
        self.assertEqual(controls, {'threshold': 0})
        with self.assertRaises(AttributeError):
            routine.values.threshold = 1
        with self.assertRaises(AttributeError):
            del routine.values.threshold

        # A deleted widget is left out, and its name is free again.
        routine.box.deleteLater()
        QtCore.QCoreApplication.sendPostedEvents(None, QtCore.QEvent.Type.DeferredDelete)
        self.assertEqual(read_saved_widgets(routine), ((), {}))
        routine.saved_widgets(QtWidgets.QSpinBox(objectName='threshold'))

    def test_bad_names_duplicate_names_and_unsupported_values_are_errors(self):
        routine = construct(lyse.Routine, {}, RoutineWindow(), 'routine.py', None)
        box = QtWidgets.QSpinBox(objectName='threshold')
        # Numbers, strings and booleans are saved, and the same widget twice is no duplicate.
        routine.saved_widgets(box, box, QtWidgets.QDoubleSpinBox(objectName='scale'),
                              QtWidgets.QLineEdit(objectName='label'),
                              QtWidgets.QCheckBox(objectName='enabled'))
        for kind, name in [(QtWidgets.QSpinBox, 'two words'), (QtWidgets.QSpinBox, ''),
                           (QtWidgets.QSpinBox, 'class'), (QtWidgets.QSpinBox, '_hidden'),
                           (QtWidgets.QLineEdit, 'threshold'), (QtWidgets.QDateEdit, 'day'),
                           (QtWidgets.QWidget, 'frame')]:
            with self.subTest(kind=kind.__name__, name=name), self.assertRaises(ValueError):
                routine.saved_widgets(kind(objectName=name))


class OneFigure(lyse.Routine):
    def __init__(self):
        self.figure = self.add_figure('Counts')


class WindowTests(unittest.TestCase):

    def setUp(self):
        self.pyplot_figures = plt.get_fignums()
        self.window = RoutineWindow()
        self.routine = construct(OneFigure, {}, self.window, 'routine.py', None)
        self.dock = self.window.findChild(QtWidgets.QDockWidget, 'Counts')

    def test_a_named_figure_is_made_once_and_kept_when_hidden(self):
        figure = self.routine.figure
        self.window.show()
        self.assertIs(self.routine.add_figure('Counts'), figure)
        self.assertEqual(plt.get_fignums(), self.pyplot_figures)

        self.dock.close()
        self.assertIs(self.routine.add_figure('Counts'), figure)
        self.assertFalse(self.dock.isVisible())
        view = next(a for a in self.window.menuBar().actions() if a.text() == 'View').menu()
        next(a for a in view.actions() if a.text() == 'Counts').trigger()
        self.assertTrue(self.dock.isVisible())

        # Refused and then hidden, so that Qt does not count the last window closed and quit.
        self.assertFalse(self.window.close())
        self.assertFalse(self.window.isVisible())
        self.assertIs(self.routine.add_figure('Counts'), figure)

    def test_ctrl_c_copies_the_figure_only_from_within_its_dock(self):
        box = QtWidgets.QLineEdit('text', self.window)
        button = QtWidgets.QPushButton(self.window)
        self.window.show()
        self.window.activateWindow()
        qapplication.processEvents()
        box.selectAll()
        qapplication.clipboard().clear()
        with mock.patch('zprocess.start_daemon') as daemon:
            # A line edit keeps Ctrl+C for itself, so only a button tells a dock's shortcut
            # from a window's.
            for widget, copies in [(box, 0), (button, 0), (self.routine.figure.canvas, 1)]:
                widget.setFocus()
                self.assertIs(qapplication.focusWidget(), widget)
                QtTest.QTest.keyClick(widget, QtCore.Qt.Key.Key_C,
                                      QtCore.Qt.KeyboardModifier.ControlModifier)
                self.assertEqual(daemon.call_count, copies)
        self.assertEqual(qapplication.clipboard().text(), 'text')
        Path(daemon.call_args.args[0][-1]).unlink()

    def test_the_layout_comes_back_with_the_output_dock_shown(self):
        second = RoutineWindow()
        construct(OneFigure, {}, second, 'routine.py', None)
        self.window.show()
        self.window.resize(500, 400)
        self.dock.close()
        self.window.dock_output.close()

        restore_layout(second, save_layout(self.window))
        second.show()
        self.assertFalse(second.findChild(QtWidgets.QDockWidget, 'Counts').isVisible())
        self.assertTrue(second.dock_output.isVisible())
        self.assertEqual(second.size(), QtCore.QSize(500, 400))

        # Damaged state is reported and ignored, and the Output dock is still shown.
        second.dock_output.close()
        with contextlib.redirect_stderr(io.StringIO()) as report:
            restore_layout(second, {'state': b'damaged'})
        self.assertIn('state', report.getvalue())
        self.assertTrue(second.dock_output.isVisible())

    def test_load_ui_finds_a_relative_file_beside_the_routine_file(self):
        folder = Path(self.enterContext(tempfile.TemporaryDirectory()))
        (folder / 'controls.ui').write_text(
            '<ui version="4.0"><class>Form</class><widget class="QWidget" name="Form">'
            '<widget class="QLineEdit" name="label"/></widget></ui>')
        (folder / 'elsewhere').mkdir()

        class Analysis(lyse.Routine):
            def __init__(self):
                self.ui = self.load_ui('controls.ui')

        with contextlib.chdir(folder / 'elsewhere'):
            routine = construct(Analysis, {}, self.window, folder / 'routine.py', None)
        self.assertIs(self.window.centralWidget(), routine.ui)
        self.assertIsInstance(routine.ui.label, QtWidgets.QLineEdit)


class SettingsTests(unittest.TestCase):

    def setUp(self):
        self.folder = Path(self.enterContext(tempfile.TemporaryDirectory()))

    def test_settings_are_kept_per_full_path_and_bad_entries_are_dropped(self):
        first = RoutineSettings(self.folder / 'a' / 'fit.py', self.folder)
        second = RoutineSettings(self.folder / 'b' / 'fit.py', self.folder)
        first.save({'threshold': 3, 'on': True}, {'geometry': b'\x01\x02'})
        second.save({'threshold': 5.5}, {})
        self.assertEqual(first.load(), ({'threshold': 3, 'on': True}, {'geometry': b'\x01\x02'}))
        self.assertEqual(second.load(), ({'threshold': 5.5}, {}))

        first.path.write_text('[controls]\nthreshold = 4\nmode = [1, 2]\n'
                              '[layout]\ngeometry = "AQI="\nstate = "!!!"\n')
        with contextlib.redirect_stderr(io.StringIO()) as report:
            self.assertEqual(first.load(), ({'threshold': 4}, {'geometry': b'\x01\x02'}))
        self.assertIn('mode', report.getvalue())
        self.assertIn('state', report.getvalue())

    def test_an_unreadable_file_is_set_aside_and_saving_resumes(self):
        settings = RoutineSettings(self.folder / 'fit.py', self.folder)
        with contextlib.redirect_stderr(io.StringIO()):
            for damaged in ('first = [', 'second = ['):
                settings.path.write_text(damaged)
                self.assertEqual(settings.load(), ({}, {}))
        self.assertEqual({path.read_text() for path in self.folder.iterdir()},
                         {'first = [', 'second = ['})
        settings.save({'threshold': 3}, {})
        self.assertEqual(settings.load(), ({'threshold': 3}, {}))

        (self.folder / 'file').write_text('')
        unwritable = RoutineSettings(self.folder / 'fit.py', self.folder / 'file')
        with contextlib.redirect_stderr(io.StringIO()) as report:
            unwritable.save({'threshold': 3}, {})
        self.assertTrue(report.getvalue())


class OutputTests(unittest.TestCase):

    def setUp(self):
        # A worker's sys.stdout and sys.stderr are descriptors 1 and 2, where native output goes,
        # but pytest's capture puts them on files of its own.
        self.enterContext(mock.patch('sys.stdout', sys.__stdout__))
        self.enterContext(mock.patch('sys.stderr', sys.__stderr__))

    def tearDown(self):
        # One cleanup each, so that a disconnection that fails cannot strand the other stream.
        for interceptor in OutputInterceptor.streams_connected.values():
            if interceptor:
                self.addCleanup(interceptor.disconnect)

    def test_output_reaches_the_output_box_and_not_lyse(self):
        config = get_config()
        # Made before any output is connected, because Qt writes warnings of its own to stderr
        # as it first sets up fonts.
        container, window = QtWidgets.QWidget(), RoutineWindow()
        lyse_box = OutputBox(QtWidgets.QVBoxLayout(container))
        self.addCleanup(lyse_box.shutdown)

        # As a worker starts, with its output going to lyse's box.
        for name in ('stdout', 'stderr'):
            OutputInterceptor(
                'localhost', lyse_box.port, name, shared_secret=config['shared_secret'],
                allow_insecure=config['allow_insecure']).connect()
        startup = dict(OutputInterceptor.streams_connected)
        box = route_output(window)
        self.addCleanup(box.shutdown)
        # Only one interceptor can be connected to a stream, so lyse's has been replaced.
        for name, interceptor in startup.items():
            self.assertNotIn(OutputInterceptor.streams_connected[name], (None, interceptor), name)
        routine = construct(lyse.Routine, {}, window, 'routine.py', box.port)
        self.assertEqual(routine.output_port, box.port)

        print('python out')
        print('python err', file=sys.stderr)
        os.write(1, b'native out\n')
        os.write(2, b'native err\n')
        expected = {'python out', 'python err', 'native out', 'native err'}
        for _ in range(500):
            shown = set(box.output_textedit.toPlainText().splitlines())
            if shown >= expected:
                break
            QtTest.QTest.qWait(10)
        self.assertLessEqual(expected, shown)
