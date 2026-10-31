#####################################################################
#                                                                   #
# /communication.py                                                 #
#                                                                   #
# Copyright 2013, Monash University                                 #
#                                                                   #
# This file is part of the program lyse, in the labscript suite     #
# (see http://labscriptsuite.org), and is licensed under the        #
# Simplified BSD License. See the license.txt file in the root of   #
# the project for the full license.                                 #
#                                                                   #
#####################################################################
"""Code required for interprocess communication
"""

import os

# 3rd party imports:
import pandas

# Labscript imports
from labscript_utils.ls_zprocess import ZMQServer
import labscript_utils.shared_drive as shared_drive

# qt imports
from qtutils import inmain_decorator
from qtutils.qt import QtCore

# Lyse imports
import lyse.routines
from lyse.dataframe_utilities import rangeindex_to_multiindex
from lyse.utils import GUI_ROUTINE_SUFFIX, LYSE_PORT

# The columns that rangeindex_to_multiindex() indexes the rows by, with and without
# integer indexing:
INDEX_COLUMNS = ['sequence', 'run time', 'sequence_index', 'run number', 'run repeat']

class LyseServer(ZMQServer):

    server = 'lyse'
    default_port = LYSE_PORT

    def __init__(self, app, *args, **kwargs):
        self.app = app
        super().__init__(*args, **kwargs)

    def _box(self, box):
        boxes = {'singleshot': self.app.singleshot_routinebox, 'multishot': self.app.multishot_routinebox}
        if box not in boxes:
            raise ValueError(f"No routine box {box!r}: it is 'singleshot' or 'multishot'")
        return boxes[box]

    @inmain_decorator()
    def handle_get_routines(self, box):
        """List a routine box's routines, 'singleshot' or 'multishot', in the
        order they run, each with whether it is active and how its last run
        ended."""
        routines = []
        for routine in self._box(box).routines:
            status = ('working' if routine.working else 'done' if routine.done
                      else 'error' if routine.error else None)
            routines.append({'filepath': routine.filepath, 'active': routine.enabled(), 'status': status})
        return routines

    @inmain_decorator()
    def handle_add_routines(self, box, filepaths, active=True):
        """Add routines, .py scripts or .lyse folders, to the end of a box; give
        paths on lyse's computer."""
        # Absolute, since a routine's worker works in the routine's folder:
        filepaths = [os.path.abspath(filepath) for filepath in filepaths]
        for filepath in filepaths:
            if not (os.path.isfile(filepath) and filepath.endswith('.py')
                    or os.path.isdir(filepath) and filepath.endswith(GUI_ROUTINE_SUFFIX)):
                raise ValueError(
                    f'Not a routine: {filepath} is neither a .py file nor a {GUI_ROUTINE_SUFFIX} folder')
        checked = QtCore.Qt.CheckState.Checked if active else QtCore.Qt.CheckState.Unchecked
        lyse.routines.add_routines([(self._box(box), [(filepath, checked) for filepath in filepaths], False)])

    @inmain_decorator()
    def handle_remove_routines(self, box, filepaths):
        """Remove routines from a box, ending their workers."""
        self._box(box).remove_routines(filepaths)

    @inmain_decorator()
    def handle_set_routines_active(self, box, filepaths, active):
        """Make routines of a box active or inactive; an inactive routine is
        skipped."""
        self._box(box).set_routines_active(filepaths, active)

    @inmain_decorator()
    def handle_reorder_routines(self, box, filepaths):
        """Set the order a box's routines run in; name every routine of the box
        once."""
        self._box(box).reorder_routines(filepaths)

    @inmain_decorator()
    def handle_restart_routines(self, box, filepaths):
        """Restart routines' workers, as after editing a routine's imported
        modules."""
        self._box(box).restart_routines(filepaths)

    def handle_get_analysis_paused(self):
        """Return whether analysis is paused."""
        return self.app.filebox.analysis_paused

    def handle_set_analysis_paused(self, paused):
        """Pause or resume analysis. A paused lyse still takes shots in, and
        analyses them on resuming."""
        self.app.filebox.set_analysis_paused(paused)

    @inmain_decorator()
    def handle_abort(self):
        """Interrupt the routines running and pause analysis, as the Abort
        button does."""
        self.app.filebox.abort()

    @inmain_decorator()
    def handle_run_multishot(self):
        """Run the multishot routines now, even while analysis is paused."""
        self.app.filebox.run_multishot()

    @inmain_decorator()
    def handle_rerun_failed(self):
        """Analyse again every shot that failed, stopped or was aborted, or
        whose multishot pass failed."""
        self.app.filebox.rerun_failed()

    def handle_get_stop_on_failure(self):
        """Return whether a shot's later single-shot routines are skipped after
        one fails."""
        return self.app.filebox.stop_on_failure

    def handle_set_stop_on_failure(self, stop):
        """Set whether a shot's later single-shot routines are skipped after one
        fails."""
        self.app.filebox.set_stop_on_failure(stop)

    @inmain_decorator()
    def handle_rerun_shots(self, filepaths):
        """Analyse these shots again, in order."""
        self.app.filebox.rerun_shots([shared_drive.path_to_local(path) for path in filepaths])

    @inmain_decorator()
    def handle_shot_status(self, filepaths):
        """Return each shot's analysis state by path: not done, awaiting
        multishot, done, failed, stopped, aborted or deleted."""
        states = self.app.filebox.shots_model.shot_states(
            [shared_drive.path_to_local(path) for path in filepaths])
        return dict(zip(filepaths, states))

    @inmain_decorator()
    def handle_remove_shots(self, filepaths):
        """Remove shots from lyse's table; their files are kept."""
        self.app.filebox.shots_model.remove_shots([shared_drive.path_to_local(path) for path in filepaths])

    @inmain_decorator()
    def handle_save_dataframe(self, folder=None):
        """Save the dataframe, one file per sequence, in folder or else beside
        each sequence's shots; return the files written."""
        return self.app.save_dataframe(folder)

    @inmain_decorator()
    def handle_load_dataframe(self, filepath):
        """Add the shots of a saved dataframe file to the table."""
        self.app.load_dataframe(filepath)

    def handle_add_shot(self, filepath, source='remote'):
        """Queue a shot file to be added and analysed; source is 'runmanager' or
        'remote'."""
        if source not in ('remote', 'runmanager'):
            raise ValueError(f"A shot's source is 'remote' or 'runmanager', not {source!r}")
        self.app.filebox.incoming_queue.put((shared_drive.path_to_local(filepath), source))

    def handle_get_dataframe(self, n_sequences=None, filter_kwargs=None, where=None, columns=None):
        """Return the dataframe, or the rows and columns chosen as lyse.data()
        chooses them."""
        df = rangeindex_to_multiindex(self._copy_dataframe(n_sequences, where, columns), inplace=True)
        if filter_kwargs is not None:
            df = df.filter(**filter_kwargs)
        # Typed here, not as each row changes, since only a client needs it:
        return df.infer_objects()

    @inmain_decorator(wait_for_return=True)
    def _copy_dataframe(self, n_sequences=None, where=None, columns=None):
        """Copy the rows and columns of lyse's dataframe that the arguments
        choose, as lyse.data takes them, in the main thread, where the dataframe
        is changed. Only those are copied, so a request for a few copies few."""
        df = self.app.filebox.shots_model.dataframe
        # Named before the rows are chosen, so that a bad name raises whatever rows remain:
        labels = None if columns is None or df.empty else self._column_labels(df, columns)
        if n_sequences is not None:
            df = self._extract_n_sequences_from_df(df, n_sequences)
        df = self._select_rows(df, where)
        if labels is not None:
            df = df[labels].sort_index(axis=1)
        return df.copy(deep=True)

    def _column_labels(self, df, columns):
        """The labels of df's columns that columns names, with the columns that
        index the rows. A name is a string or a tuple of strings, and names a
        column, else every column whose label begins with it."""
        labels = []
        for name in columns:
            key = (name,) if isinstance(name, str) else tuple(name)
            padded = key + ('',) * (df.columns.nlevels - len(key))
            matches = [padded] if padded in df.columns else [
                label for label in df.columns if label[:len(key)] == key]
            if not matches:
                raise KeyError(f'No column {name!r} in the lyse dataframe.')
            labels.extend(matches)
        padding = ('',) * (df.columns.nlevels - 1)
        labels += [(name,) + padding for name in INDEX_COLUMNS if (name,) + padding in df.columns]
        return list(dict.fromkeys(labels))

    def _select_rows(self, df, where):
        """The rows of df matching where, a dict of {column: value}."""
        for key, value in (where or {}).items():
            # A string names a top-level column, a tuple a nested one:
            column = (key,) if isinstance(key, str) else tuple(key)
            column += ('',) * (df.columns.nlevels - len(column))
            if column not in df.columns:
                raise ValueError(f'No column {key!r} in the lyse dataframe.')
            if pandas.api.types.is_list_like(value):
                df = df[df[column].isin(value)]
            else:
                df = df[df[column] == value]
        return df

    def _extract_n_sequences_from_df(self, df, n_sequences):
        if df.empty or n_sequences == 0:
            return df.iloc[:0]
        # One engage in runmanager makes one sequence, identified by its 'sequence' time,
        # 'sequence_index' (apart calls within one second) and 'labscript'. Sorting the
        # identities orders sequences oldest first, whatever the order of the rows.
        identities = [df['sequence'], df['sequence_index'].fillna(-1).astype(int),
                      df['labscript'].astype(str)]
        numbers = df.groupby(identities, sort=True, dropna=False).ngroup()
        return df[(numbers > numbers.max() - n_sequences).to_numpy()]
