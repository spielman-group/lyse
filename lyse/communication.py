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

class LyseServer(ZMQServer):

    server = 'lyse'
    default_port = LYSE_PORT

    def __init__(self, app, *args, **kwargs):
        self.app = app
        super().__init__(*args, **kwargs)

    def handle_add_shot(self, filepath):
        self.app.filebox.incoming_queue.put(shared_drive.path_to_local(filepath))

    def handle_get_dataframe(self, n_sequences=None, filter_kwargs=None, where=None):
        if n_sequences is None:
            # where chooses the rows before the dataframe is copied, so
            # that a request for a few rows copies only those:
            df = self._copy_dataframe(where)
        else:
            # where applies after n_sequences, which, run in the main
            # thread where the copy is made, would cost more than it saves:
            df = self._extract_n_sequences_from_df(self._copy_dataframe(), n_sequences)
            df = self._select_rows(df, where)
        df = rangeindex_to_multiindex(df, inplace=True)
        if filter_kwargs is not None:
            df = df.filter(**filter_kwargs)
        # Typed here, not as each row changes, since only a client needs it:
        return df.infer_objects()

    @inmain_decorator(wait_for_return=True)
    def _copy_dataframe(self, where=None):
        """Copy the rows of lyse's dataframe that where chooses, or all of
        them, in the main thread, where the dataframe is changed."""
        return self._select_rows(self.app.filebox.shots_model.dataframe, where).copy(deep=True)

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
        # An empty dataframe has no columns to read below, which would raise a KeyError.
        if df.empty:
            return df

        # One engage in runmanager makes one sequence, identified by its 'sequence' time,
        # 'sequence_index' (apart calls within one second) and 'labscript'. Sorting the
        # identities orders sequences oldest first, whatever the order of the rows.
        identities = [
            (sequence, -1 if pandas.isna(index) else int(index), str(labscript))
            for sequence, index, labscript
            in zip(df['sequence'], df['sequence_index'], df['labscript'])
        ]

        unique_identities = sorted(set(identities))

        # Keep the last n_sequences sequences. A slice longer than unique_identities
        # returns all of it, but [-0:] does too, so 0 needs its own branch.
        if n_sequences == 0:
            identities_included = set()
        else:
            identities_included = set(unique_identities[-n_sequences:])
        df_subset = df[[id in identities_included for id in identities]]

        return df_subset
