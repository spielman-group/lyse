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
import logging
import queue
import threading

import labscript_utils.h5_lock
from labscript_utils.ls_zprocess import Lock
from labscript_utils.shared_drive import path_to_agnostic
from qtutils import inmain

from lyse.plugins import BasePlugin

logger = logging.getLogger('lyse.plugins.delete_default_and_repeated_shots')


class Plugin(BasePlugin):

    def __init__(self):
        self.shots = None
        self.output_box = None
        self.from_runmanager = set()
        self.deletions = queue.Queue()
        # Files are deleted on a thread of their own, which waits for each file's lock:
        self.thread = threading.Thread(target=self.delete_loop, daemon=True)
        self.thread.start()

    def plugin_setup_complete(self, data):
        self.shots = data['shots_model']
        self.output_box = data['output_box']

    def get_event_handlers(self):
        return {'shot_received': self.on_shot_received, 'analysis_done': self.on_analysis_done}

    def on_shot_received(self, filepath, source):
        if source == 'runmanager':
            self.from_runmanager.add(filepath)
        else:
            self.from_runmanager.discard(filepath)

    def on_analysis_done(self, filepath, outcomes):
        if filepath in self.from_runmanager and all(outcome == 'done' for outcome in outcomes.values()):
            self.deletions.put(filepath)

    def claim(self, filepath):
        """Mark the shot's row deleted, and return True, if its file is to go: it came
        from runmanager, its analysis is done with every outcome written into its file,
        and it is a default shot or a repeat. On the GUI thread, in one call, so that
        nothing changes the row between the check and the mark."""
        row = self.shots.row_number_by_filepath.get(filepath)
        if (row is None or filepath not in self.from_runmanager or self.shots.states[row] != 'done'
                or not self.shots.outcomes_recorded.get(filepath)):
            return False
        df = self.shots.dataframe
        if not (df['sequence_index'].iloc[row] == -1 or df['run repeat'].iloc[row] >= 1):
            return False
        self.shots.mark_deleted(filepath, warn=False)
        return True

    def delete_loop(self):
        for filepath in iter(self.deletions.get, None):
            try:
                with Lock(path_to_agnostic(filepath)):
                    if inmain(self.claim, filepath):
                        try:
                            labscript_utils.h5_lock.delete_shot_file(filepath)
                        except OSError as e:
                            inmain(self.shots.end_attempt, filepath, 'done')
                            self.output_box.output(f'Warning: Could not delete {filepath}: {e}\n', red=True)
            except Exception:
                logger.exception('Could not delete %s', filepath)

    def close(self):
        # Lets the deletions already queued finish:
        self.deletions.put(None)
        self.thread.join()
