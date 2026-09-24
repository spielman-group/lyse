"""The lyse main window: a theme change, and saving the dataframe.

``lyse.__main__`` builds a ``Splash`` and calls ``.show()`` at module scope, so
importing it would put a banner on the screen of whoever runs the tests. The
splash module is stubbed before the import, so that no QApplication is created
and nothing is shown, while the module's classes stay borrowable.
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
from qtutils.qt import QtGui, QtWidgets

from lyse.dataframe_utilities import flat_dict_to_hierarchical_dataframe, flatten_dict


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
    # nothing.
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


class Child(QtWidgets.QWidget):
    """Counts the repaints the window gives it."""
    repaints = 0

    def setStyleSheet(self, sheet):
        self.repaints += 1
        super().setStyleSheet(sheet)


class MainWindowTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.qapplication = a_qapplication()
        cls.main = import_main_without_splash()

    def test_a_theme_change_repaints_the_window_without_error(self):
        """The window repaints its widgets when the application palette
        changes, as it does on an OS light/dark switch. The handler is a Qt
        virtual method, so a failure in it reaches sys.excepthook."""
        window = self.main.LyseMainWindow(None)
        child = Child(window)
        seen = []
        original_hook, original_palette = sys.excepthook, self.qapplication.palette()
        sys.excepthook = lambda cls, exc, tb: seen.append(exc)
        try:
            palette = QtGui.QPalette(original_palette)
            palette.setColor(QtGui.QPalette.ColorRole.Window, QtGui.QColor('#202020'))
            self.qapplication.setPalette(palette)
            self.qapplication.processEvents()
        finally:
            sys.excepthook = original_hook
            self.qapplication.setPalette(original_palette)
        self.assertEqual(seen, [])
        self.assertGreater(child.repaints, 0)

    def test_a_dataframe_with_object_columns_is_saved_beside_its_shots(self):
        """Image attributes arrive as bytes and some results as arrays, so a
        real dataframe always has columns that cannot be made numeric."""
        with tempfile.TemporaryDirectory() as folder:
            df = flat_dict_to_hierarchical_dataframe(flatten_dict({
                'sequence': pandas.Timestamp('2026-09-22 10:00:00', tz='UTC'),
                'labscript': 'experiment.py',
                'filepath': os.path.join(folder, 'shot.h5'),
                'side': {'absorption': {'atoms': {'CLASS': numpy.bytes_(b'IMAGE')}}},
                'routine': {'best': numpy.array([1.0, 2.0])},
            }))
            (saved,) = self.save(folder, df)
            self.assertEqual(list(pandas.read_pickle(saved)['filepath']), [df['filepath'][0]])

    def test_labscripts_engaged_in_the_same_second_are_saved_apart(self):
        with tempfile.TemporaryDirectory() as folder:
            df = pandas.concat([flat_dict_to_hierarchical_dataframe(flatten_dict({
                'sequence': pandas.Timestamp('2026-09-22 10:00:00', tz='UTC'),
                'labscript': script,
                'filepath': os.path.join(folder, script + '.h5'),
            })) for script in ('a.py', 'b.py')], ignore_index=True)
            self.assertEqual([os.path.basename(path) for path in self.save(folder, df)],
                             ['dataframe_20260922T100000_a.pkl', 'dataframe_20260922T100000_b.pkl'])

    def save(self, folder, df):
        """Save df as lyse does, beside its shots, and return the files written."""
        app = types.SimpleNamespace(
            filebox=types.SimpleNamespace(shots_model=types.SimpleNamespace(dataframe=df)),
            exp_config=types.SimpleNamespace(get=lambda section, option: folder),
        )
        self.main.Lyse.on_save_dataframe_triggered(app, choose_folder=False)
        return sorted(glob.glob(os.path.join(folder, '*.pkl')))


if __name__ == '__main__':
    unittest.main()
