#####################################################################
#                                                                   #
# /tests/test_main_window.py                                        #
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
"""The lyse main window: a theme change, its plugins, and saving and loading the
dataframe."""
import logging
import os
import queue
import sys
import tempfile
import types
import unittest
from unittest import mock

import numpy
import pandas
from labscript_utils.labconfig import save_appconfig
from labscript_utils.qtwidgets.leftclick import TableView
from qtutils import UiLoader
from qtutils.qt import QtGui, QtWidgets

import lyse.filebox
import lyse.plugins
import lyse.utils
from lyse.dataframe_utilities import flat_dict_to_hierarchical_dataframe, flatten_dict


def a_qapplication():
    """One QApplication for the whole process, as Qt requires."""
    return QtWidgets.QApplication.instance() or QtWidgets.QApplication(['test'])


# At module scope: lyse.__main__ is imported once per process, and the splash
# it builds is an instance of the class current at that import.
class FakeSplash:
    """Stands in for ``labscript_utils.splash.Splash``, and shows nothing."""

    def __init__(self, *args, **kwargs):
        pass

    def show(self):
        pass

    def hide(self):
        pass

    def update_text(self, text):
        pass


def import_main_without_splash():
    """Import ``lyse.__main__`` with a splash that does nothing."""
    fake = types.ModuleType('labscript_utils.splash')
    fake.Splash = FakeSplash
    fake.get_qapplication = lambda *args, **kwargs: None

    # The fake goes in sys.modules and on the parent package: lyse.__main__'s
    # import is satisfied by sys.modules, but `labscript_utils.splash.Splash`
    # then reads the package attribute, the real module if already imported.
    import labscript_utils

    saved_module = sys.modules.get('labscript_utils.splash')
    had_attr = hasattr(labscript_utils, 'splash')
    saved_attr = getattr(labscript_utils, 'splash', None)

    sys.modules['labscript_utils.splash'] = fake
    labscript_utils.splash = fake
    try:
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
        """The window repaints its widgets when the palette changes, as on an OS
        light/dark switch. A failure in the Qt handler reaches sys.excepthook."""
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

    def test_a_saved_dataframe_loads_beside_the_shots_already_there(self):
        """A shot changed since saving is read again from its file."""
        with tempfile.TemporaryDirectory() as folder:
            engaged = pandas.Timestamp('2026-09-22 10:00:00', tz='UTC')
            df = pandas.concat([flat_dict_to_hierarchical_dataframe(flatten_dict({
                'sequence': engaged,
                'labscript': 'experiment.py',
                'run time': engaged + pandas.Timedelta(seconds=n),
                'filepath': os.path.join(folder, f'{n}.h5'),
                'fit': {'atoms': 100.0 + n},
                'lyse': {'fit': 'error' if n == 2 else 'done'},
            })) for n in range(4)], ignore_index=True)
            # There, but not changed since saving:
            open(df['filepath'][2], 'w').close()
            (saved,) = self.save(folder, df)
            changed = df['filepath'][0]
            open(changed, 'w').close()
            os.utime(changed, (os.path.getmtime(saved) + 1,) * 2)
            app = types.SimpleNamespace(
                qapplication=self.qapplication, ui=None,
                output_box=types.SimpleNamespace(output=lambda *args, **kwargs: None),
                exp_config=types.SimpleNamespace(get=lambda *args: folder),
                filebox=types.SimpleNamespace(add_shots_start_time=None, incoming_queue=queue.Queue(),
                                              fire_event=lambda *args: None,
                                              set_add_shots_progress=lambda *args: None))
            shots = app.filebox.shots_model = lyse.filebox.DataFrameModel(
                app, QtWidgets.QTableView(), types.SimpleNamespace(getboolean=lambda *args: False))
            shots.add_files([df['filepath'][1]], df.iloc[[1]], ['remote'], done=True)
            self.main.Lyse.load_dataframe(app, saved)
            self.assertEqual(shots.filepaths, list(df['filepath'][1:]))
            # A loaded row takes its state from its outcomes, unless its file is not there:
            self.assertEqual(shots.states, ['done', 'failed', 'deleted'])
            self.assertEqual(list(shots.dataframe['fit', 'atoms']), [101.0, 102.0, 103.0])
            self.assertEqual(app.filebox.incoming_queue.get_nowait(), (changed, 'dataframe'))

    def test_a_plugin_gets_its_saved_data_then_its_place_in_the_window(self):
        events, widget = [], QtWidgets.QLabel('probe')

        class Plugin(lyse.plugins.BasePlugin):
            def restore_save_data(self, data):
                events.append(('restored', data))

            def get_ui_contributions(self):
                return [{'context': 'areas', 'area': 'shots_toolbar', 'widget': widget}]

            def get_menu_contributions(self):
                return [{'location': 'menubar', 'path': 'Probe', 'name': 'Act', 'action': lambda: None}]

            def plugin_setup_complete(self, data):
                events.append(('set up', data['shots_model']))

        module = types.ModuleType('probe')
        module.Plugin = Plugin
        loader = UiLoader()
        loader.registerCustomWidget(TableView)
        app = types.SimpleNamespace(
            ui=loader.load(os.path.join(lyse.utils.LYSE_DIR, 'user_interface/main.ui')),
            filebox=types.SimpleNamespace(
                ui=loader.load(os.path.join(lyse.utils.LYSE_DIR, 'user_interface/filebox.ui')),
                shots_model='shots'),
            singleshot_routinebox=None, multishot_routinebox=None, logger=logging.getLogger('test'))
        manager = lyse.plugins.manager
        with tempfile.TemporaryDirectory() as folder, \
                mock.patch.multiple(manager, modules={'probe': module}, plugins={}, contexts={}):
            config = os.path.join(folder, 'lyse.toml')
            save_appconfig(config, {'lyse_state': {'plugins': {'probe': {'n': 3}}}})
            app.exp_config = types.SimpleNamespace(get=lambda *args: config)
            self.main.Lyse.setup_plugins(app)
        self.assertEqual(events, [('restored', {'n': 3}), ('set up', 'shots')])
        self.assertIs(app.filebox.ui.horizontalLayout_shots_toolbar_plugins.itemAt(0).widget(), widget)
        self.assertIn('Probe', [action.text() for action in app.ui.menuBar.actions()])

    def save(self, folder, df):
        """Save df as lyse does, beside its shots, and return the files written."""
        app = types.SimpleNamespace(
            filebox=types.SimpleNamespace(shots_model=types.SimpleNamespace(dataframe=df)))
        return sorted(self.main.Lyse.save_dataframe(app))


if __name__ == '__main__':
    unittest.main()
