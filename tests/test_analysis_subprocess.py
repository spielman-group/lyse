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
"""A theme change reaching a plot window.

The window repaints its widgets when the application palette changes, which is
how it follows an OS light/dark switch. The handler is a Qt virtual method, so
a failure in it reaches sys.excepthook rather than the caller.
"""
import sys
import tempfile
import unittest
import warnings
from unittest import mock

from qtutils.qt import QtCore, QtGui, QtWidgets

with warnings.catch_warnings():
    warnings.simplefilter('ignore')
    import lyse.analysis_subprocess


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


if __name__ == '__main__':
    unittest.main()
