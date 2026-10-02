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
import io
import tempfile
import unittest
from pathlib import Path

from qtutils.qt import QtCore, QtWidgets

import lyse
from lyse.routine import (
    RoutineSettings, construct, read_saved_widgets, routine_class, routine_mode)


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

    def setUp(self):
        self.qapplication = QtWidgets.QApplication.instance() or QtWidgets.QApplication(['test'])

    def test_a_saved_widget_takes_its_saved_value_and_reaches_the_snapshot(self):
        class Analysis(lyse.Routine):
            def __init__(self):
                self.box = QtWidgets.QSpinBox(objectName='threshold', value=5)
                self.saved_widgets(self.box)

        # A saved 0 is falsy, and is still restored over the box's default of 5.
        routine = construct(Analysis, {'threshold': 0})
        self.assertEqual(routine.box.value(), 0)
        self.assertEqual(len(routine.values), 0)

        # A saved value the box rejects is reported and ignored.
        with contextlib.redirect_stderr(io.StringIO()) as report:
            rejected = construct(Analysis, {'threshold': 'text'})
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
        routine = construct(lyse.Routine, {})
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
