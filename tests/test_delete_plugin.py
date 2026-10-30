#####################################################################
#                                                                   #
# /tests/test_delete_plugin.py                                      #
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
"""The plugin delete_default_and_repeated_shots deletes the file of a default or
repeated shot from runmanager once its analysis has ended with every routine done,
and keeps its row."""
import os
import tempfile
import threading
import types
import unittest

import labscript_utils.h5_lock, h5py
import pandas
from qtutils.qt import QtWidgets

import lyse.filebox
from lyse.plugins.delete_default_and_repeated_shots import Plugin

qapplication = QtWidgets.QApplication.instance() or QtWidgets.QApplication(['test'])


class DeletePluginTests(unittest.TestCase):

    def test_only_a_default_or_repeated_shot_from_runmanager_with_its_analysis_done_goes(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        # Each shot's source, sequence index, run repeat and its routine's outcome:
        shots_made = {
            'default': ('runmanager', -1, 0, 'done'),
            'repeat': ('runmanager', 3, 1, 'done'),
            'first': ('runmanager', 3, 0, 'done'),
            'failed': ('runmanager', -1, 0, 'error'),
            'by hand': ('dialog', -1, 0, 'done'),
        }
        paths = {name: os.path.join(folder.name, f'{name}.h5') for name in shots_made}
        for path in paths.values():
            h5py.File(path, 'w').close()
        app = types.SimpleNamespace(
            output_box=types.SimpleNamespace(output=lambda *args, **kwargs: None),
            filebox=types.SimpleNamespace(add_shots_start_time=None, fire_event=lambda *args: None,
                                          set_add_shots_progress=lambda *args: None))
        shots = lyse.filebox.DataFrameModel(
            app, QtWidgets.QTableView(), types.SimpleNamespace(getboolean=lambda *args: False))
        shots.add_files(list(paths.values()), pandas.DataFrame({
            ('filepath', ''): list(paths.values()),
            ('sequence_index', ''): [made[1] for made in shots_made.values()],
            ('run repeat', ''): [made[2] for made in shots_made.values()],
            ('fit', 'atoms'): [100.0] * len(paths),
        }), ['remote'] * len(paths))
        plugin = Plugin()
        plugin.plugin_setup_complete({'shots_model': shots, 'output_box': app.output_box})
        for name, (source, _, _, outcome) in shots_made.items():
            plugin.on_shot_received(paths[name], source)
            shots.end_attempt(paths[name], lyse.filebox.outcome_state({'fit': outcome}))
            shots.outcomes_recorded[paths[name]] = True
            plugin.on_analysis_done(paths[name], {'fit': outcome})
        # As lyse closes it, off the GUI thread, which keeps serving:
        closing = threading.Thread(target=plugin.close)
        closing.start()
        while closing.is_alive():
            qapplication.processEvents()
            closing.join(0.02)
        self.assertEqual({name: os.path.exists(path) for name, path in paths.items()},
                         {'default': False, 'repeat': False, 'first': True, 'failed': True, 'by hand': True})
        self.assertEqual(shots.states, ['deleted', 'deleted', 'done', 'failed', 'done'])
        self.assertEqual(list(shots.dataframe['fit', 'atoms']), [100.0] * len(paths))


if __name__ == '__main__':
    unittest.main()
