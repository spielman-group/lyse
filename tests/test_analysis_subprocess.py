"""Theme changes reaching a plot window.

``PlotWindow.changeEvent`` runs whenever Qt tells the window something about
its appearance has changed, which on macOS happens the moment a window is
created as well as when the user switches between light and dark. It is a Qt
virtual method, so an exception in it does not stop lyse: PyQt hands it to
``sys.excepthook`` and carries on, and with ``labscript_utils.excepthook``
installed that is an error window every time a plot appears.
"""
import sys
import unittest
import warnings

from qtutils.qt import QtCore, QtWidgets

with warnings.catch_warnings():
    warnings.simplefilter('ignore')
    import lyse.analysis_subprocess


def a_qapplication():
    """One QApplication for the whole process, as Qt requires."""
    return QtWidgets.QApplication.instance() or QtWidgets.QApplication(['test'])


class RecordingChild(QtWidgets.QWidget):
    """A child that records the repaint the handler is supposed to give it.

    Asserting only that no exception escaped would not distinguish a handler
    that ran from one whose condition never matched.
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self.restyled = 0
        self.repalettes = 0

    def setStyleSheet(self, sheet):
        self.restyled += 1
        super().setStyleSheet(sheet)

    def setPalette(self, palette):
        self.repalettes += 1
        super().setPalette(palette)


class PlotWindowThemeChangeTests(unittest.TestCase):
    """The event types the window asks about have to exist, and have to arrive.

    An enum member that is merely absent raises AttributeError rather than
    comparing false, so naming one Qt does not have breaks the whole handler
    and not just the theme case it was reaching for.
    """

    def setUp(self):
        self.qapplication = a_qapplication()
        self.window = lyse.analysis_subprocess.PlotWindow(
            None, analysis_filepath='test_analysis_subprocess.py',
            analysis_identifier=0,
        )
        self.child = RecordingChild(self.window)
        self.addCleanup(self.window.deleteLater)

    def send(self, event_type):
        """Deliver a change event the way Qt does, and report what escaped.

        Through ``sendEvent`` PyQt would divert an exception in the virtual
        method to ``sys.excepthook``, so the test would pass while lyse spawned
        an error window per plot. Watch the hook for exactly that.
        """
        seen = []
        original = sys.excepthook
        sys.excepthook = lambda cls, exc, tb: seen.append(exc)
        try:
            self.child.restyled = self.child.repalettes = 0
            self.qapplication.sendEvent(self.window, QtCore.QEvent(event_type))
        finally:
            sys.excepthook = original
        return seen

    def test_a_palette_change_repaints_the_children(self):
        """What a widget actually receives when the application palette changes.

        ApplicationPaletteChange goes to the application, and QWidget.event()
        does not pass it to changeEvent -- a test sending it here would never
        reach the handler and would pass however broken the handler was.
        """
        self.assertEqual([], self.send(QtCore.QEvent.Type.PaletteChange))
        self.assertEqual(1, self.child.restyled)
        self.assertEqual(1, self.child.repalettes)

    def test_a_style_change_repaints_the_children(self):
        self.assertEqual([], self.send(QtCore.QEvent.Type.StyleChange))
        self.assertEqual(1, self.child.restyled)
        self.assertEqual(1, self.child.repalettes)

    def test_an_unrelated_change_event_repaints_nothing(self):
        """Every change event reaches the method; only these two do the work."""
        self.assertEqual([], self.send(QtCore.QEvent.Type.WindowStateChange))
        self.assertEqual(0, self.child.restyled)
        self.assertEqual(0, self.child.repalettes)
