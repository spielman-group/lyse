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

class LyseServer(ZMQServer):

    def __init__(self, app, *args, **kwargs):
        self.app = app
        super().__init__(*args, **kwargs)

    def handle_add_shot(self, filepath):
        self.app.filebox.incoming_queue.put(shared_drive.path_to_local(filepath))

    def handle_get_dataframe(self, n_sequences=None, filter_kwargs=None, where=None):
        # Return only a subset of the dataframe if instructed to do so.
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
        return df

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
        # If the dataframe is empty, just return it, otherwise accessing columns
        # below will raise a KeyError.
        if df.empty:
            return df

        # Get a list of all unique sequences, each corresponding to one call to
        # engage in runmanager. Each sequence may contain multiple runs. The
        # below creates tuples to identify sequences. To be from the same
        # sequence, two shots have to have the same value for 'sequence' (which
        # makes sure that the time when engage was called are the same to within
        # 1 second), 'labscript' (must have been generated from the same
        # labscript), and 'sequence_index' (a counter which keeps track of how
        # many times engage has been called for that labscript and resets to 0
        # at the start of each day). Typically just the value for sequence, is
        # enough. However it only records time down to the second, so if
        # engage() is called twice quickly then two different sequences can end
        # up with the same value there.
        #
        # Sorting the tuples orders sequences by 'sequence', then within one
        # second by 'sequence_index' as an integer, then by 'labscript' to
        # break ties; the order of the rows plays no part.
        identities = [
            (sequence, -1 if pandas.isna(index) else int(index), str(labscript))
            for sequence, index, labscript
            in zip(df['sequence'], df['sequence_index'], df['labscript'])
        ]

        # Find the distinct sequences, oldest first.
        unique_identities = sorted(set(identities))

        # Slice the DataFrame so that only the last n_sequences sequences
        # remain. Note that slicing unique_identities just returns all of its
        # entries if n_sequences is greater than its length; it doesn't raise an
        # error.
        if n_sequences == 0:
            identities_included = set()
        else:
            identities_included = set(unique_identities[-n_sequences:])
        df_subset = df[[id in identities_included for id in identities]]

        return df_subset