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
"""The mode a routine file declares, and the routine class it defines."""
import unittest

import lyse
from lyse.routine import routine_class, routine_mode


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
