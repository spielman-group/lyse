"""Behaviour of ``lyse.__main__``: theme changes and saving the dataframe.

``LyseMainWindow.changeEvent`` repaints every child widget when the
application's appearance changes, which is how lyse follows an OS light/dark
switch. It is a Qt virtual method, so anything that goes wrong in it is handed
to ``sys.excepthook`` rather than stopping lyse -- and an event the window
never receives fails silently in the other direction, by simply not running.
Both are tested for here.

``lyse.__main__`` builds a ``Splash`` and calls ``.show()`` at module scope, so
importing it would put a banner on the screen of whoever runs the tests. The
splash module is stubbed before the import, so that no QApplication is
created and nothing is shown, while the module's classes stay borrowable.
"""
import glob
import os
import sys
import tempfile
import types
import unittest
import warnings

import numpy
import pandas
from qtutils.qt import QtCore, QtWidgets

from lyse.dataframe_utilities import (
    concat_with_padding,
    flat_dict_to_hierarchical_dataframe,
    flatten_dict,
)


def a_qapplication():
    """One QApplication for the whole process, as Qt requires."""
    return QtWidgets.QApplication.instance() or QtWidgets.QApplication(['test'])


class FakeSplash:
    """Stands in for ``labscript_utils.splash.Splash``, and shows nothing.

    Defined once, at module scope: ``lyse.__main__`` is imported only once per
    process however many tests ask for it, so the splash it built is an
    instance of whichever class was current at that first import.
    """

    def __init__(self, *args, **kwargs):
        pass

    def show(self):
        pass

    def hide(self):
        pass

    def update_text(self, text):
        pass


def import_main_without_splash():
    """Import ``lyse.__main__`` with a splash that does nothing.

    The real ``Splash.__init__`` creates the QApplication and shows a window
    that only ``if __name__ == '__main__'`` ever hides. A fake leaves the real
    module's classes borrowable without either happening.
    """
    fake = types.ModuleType('labscript_utils.splash')
    fake.Splash = FakeSplash
    fake.get_qapplication = lambda *args, **kwargs: None

    # Both halves are needed. `import labscript_utils.splash` is satisfied by
    # sys.modules, but the `labscript_utils.splash.Splash(...)` that follows
    # reads an attribute of the parent package, which the import system would
    # normally have set. Without the setattr the real module is used whenever
    # anything else has already imported it, and the stub silently does
    # nothing -- which is how this was found.
    import labscript_utils

    saved_module = sys.modules.get('labscript_utils.splash')
    had_attr = hasattr(labscript_utils, 'splash')
    saved_attr = getattr(labscript_utils, 'splash', None)

    sys.modules['labscript_utils.splash'] = fake
    labscript_utils.splash = fake
    try:
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            import lyse.__main__ as main
    finally:
        if saved_module is None:
            del sys.modules['labscript_utils.splash']
        else:
            sys.modules['labscript_utils.splash'] = saved_module
        if had_attr:
            labscript_utils.splash = saved_attr
        else:
            del labscript_utils.splash

    # The module builds its splash at import. If the stub had been bypassed
    # this would be the real one, and a QApplication and a window would exist.
    assert isinstance(main.splash, FakeSplash), 'the real splash module was used'
    return main


class RecordingChild(QtWidgets.QWidget):
    """A child that records the repaint the handler is supposed to give it.

    The handler's whole effect is calling setStyleSheet and setPalette on every
    child. Asserting that no exception escaped does not distinguish a handler
    that ran from one whose condition never matched -- and a condition that
    never matches is the defect here, not a crash.
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


class MainWindowThemeChangeTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.qapplication = a_qapplication()
        cls.main = import_main_without_splash()

    def setUp(self):
        self.window = self.main.LyseMainWindow(None)
        self.child = RecordingChild(self.window)
        self.addCleanup(self.window.deleteLater)

    def send(self, event_type):
        """Deliver a change event the way Qt does, and report what escaped.

        Through ``sendEvent`` PyQt diverts an exception raised in the virtual
        method to ``sys.excepthook``, so a test that only sent the event would
        pass while lyse spawned an error window. Watch the hook for that.
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
        """The event Qt actually delivers when the application palette changes.

        ApplicationPaletteChange goes to the application and QWidget.event()
        does not pass it to changeEvent, so a window asking for that one never
        runs at all. Sending it here would reach nothing and pass however
        broken the handler was.
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


class SaveDataframeTests(unittest.TestCase):
    """Saving the dataframe writes one pickle per sequence beside its shots.

    Before writing, each object column is converted to numbers when every value
    in it is numeric and left alone otherwise. A real frame always has columns
    that cannot convert -- image attributes arrive as bytes and some results
    are arrays -- so the save has to pass over those rather than fail on them.
    """

    SEQUENCE = pandas.Timestamp('2026-09-22 10:00:00', tz='UTC')

    @classmethod
    def setUpClass(cls):
        a_qapplication()
        cls.main = import_main_without_splash()

    def a_shot(self, folder, n):
        return flat_dict_to_hierarchical_dataframe(flatten_dict({
            'sequence': self.SEQUENCE,
            'labscript': 'experiment.py',
            'filepath': os.path.join(folder, 'shot_{}.h5'.format(n)),
            'side': {'absorption': {'atoms': {'CLASS': numpy.bytes_(b'IMAGE')}}},
            'routine': {'count': n, 'best': numpy.array([n, n + 1.0])},
        }))

    def save(self, folder, df):
        """Run the real save handler against a stand-in for the Lyse app."""
        app = types.SimpleNamespace(
            filebox=types.SimpleNamespace(shots_model=types.SimpleNamespace(dataframe=df)),
            exp_config=types.SimpleNamespace(get=lambda section, option: folder),
        )
        self.main.Lyse.on_save_dataframe_triggered(app, choose_folder=False)
        return sorted(glob.glob(os.path.join(folder, '*.pkl')))

    def test_a_sequence_is_saved_beside_its_shots(self):
        with tempfile.TemporaryDirectory() as folder:
            df = concat_with_padding(self.a_shot(folder, 1), self.a_shot(folder, 2))
            # FileBox.update_row stores a newly created result column as object.
            count = ('routine', 'count', '', '')
            df[count] = df[count].astype(object)

            saved = self.save(folder, df)

            self.assertEqual(
                [os.path.join(folder, 'dataframe_20260922T100000_experiment.pkl')], saved)
            loaded = pandas.read_pickle(saved[0])
            self.assertEqual(2, len(loaded))
            self.assertEqual('int64', str(loaded[count].dtype))
            self.assertEqual(object, loaded['side', 'absorption', 'atoms', 'CLASS'].dtype)
            self.assertEqual(object, loaded['routine', 'best', '', ''].dtype)
