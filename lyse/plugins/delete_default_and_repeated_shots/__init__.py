#####################################################################
#                                                                   #
# /plugins/delete_default_and_repeated_shots/__init__.py            #
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
"""Deletes the file of a default or repeated shot from runmanager once all its analysis
has ended with every routine done. Its row stays, with its results, so a run of
repeated shots that keep the apparatus warm still leaves its record."""
import labscript_utils.h5_lock, h5py

from lyse.plugins import BasePlugin


class Plugin(BasePlugin):

    def __init__(self):
        self.shots = None
        self.output_box = None
        self.from_runmanager = set()
        # Whether it deletes at all, saved with lyse's configuration:
        self.enabled = True

    def plugin_setup_complete(self, data):
        self.shots = data['shots_model']
        self.output_box = data['output_box']

    def get_save_data(self):
        return {'enabled': self.enabled}

    def restore_save_data(self, data):
        self.enabled = data.get('enabled', True)

    def get_event_handlers(self):
        return {'shot_received': self.on_shot_received, 'analysis_done': self.on_analysis_done}

    def on_shot_received(self, filepath, source):
        if source == 'runmanager':
            self.from_runmanager.add(filepath)
        else:
            self.from_runmanager.discard(filepath)

    def on_analysis_done(self, filepath, outcomes):
        if (not self.enabled or filepath not in self.from_runmanager
                or any(outcome != 'done' for outcome in outcomes.values())):
            return
        try:
            with h5py.File(filepath, 'r') as f:
                default = f.attrs['sequence_index'] == -1
                repeat = f.attrs.get('run repeat', 0) >= 1
            if default or repeat:
                labscript_utils.h5_lock.delete_shot_file(filepath)
                self.shots.mark_deleted(filepath, warn=False)
        except OSError as e:
            self.output_box.output(f'Warning: Could not delete {filepath}: {e}\n', red=True)
