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

# 3rd party imports:
import pandas

# Labscript imports
from labscript_utils.ls_zprocess import ZMQServer
import labscript_utils.shared_drive as shared_drive

# qt imports
from qtutils import inmain_decorator

# Lyse imports
from lyse.dataframe_utilities import rangeindex_to_multiindex
from lyse.utils import LYSE_PORT

# The columns that rangeindex_to_multiindex() indexes the rows by, with and without
# integer indexing:
INDEX_COLUMNS = ['sequence', 'run time', 'sequence_index', 'run number', 'run repeat']

class LyseServer(ZMQServer):

    server = 'lyse'
    default_port = LYSE_PORT

    def __init__(self, app, *args, **kwargs):
        self.app = app
        super().__init__(*args, **kwargs)

    def handle_add_shot(self, filepath):
        self.app.filebox.incoming_queue.put(shared_drive.path_to_local(filepath))

    def handle_get_dataframe(self, n_sequences=None, filter_kwargs=None, where=None, columns=None):
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
