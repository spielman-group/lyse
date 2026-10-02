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
"""The mode a routine file declares."""
import unittest

from lyse.routine import routine_mode


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
