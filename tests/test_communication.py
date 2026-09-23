"""lyse.data(n_sequences=N) returns the N most recently engaged sequences.

Most recent means most recently engaged in runmanager, not most recently loaded
into lyse, and two engages in the same second are ordered by sequence_index.
lyse's integer_indexing setting decides the order the rows are held in, so the
answer must not depend on it.
"""
import logging
import types
import unittest
from unittest import mock

import pandas

import lyse
from lyse import dataframe_utilities
from lyse.communication import WebServer
from lyse.dataframe_utilities import (
    asdatetime,
    concat_with_padding,
    flat_dict_to_hierarchical_dataframe,
    flatten_dict,
)


def a_sequence(engaged, sequence_index):
    """The rows lyse makes for the two shots of one engage."""
    return [
        flat_dict_to_hierarchical_dataframe(flatten_dict({
            'filepath': f'{engaged}_{sequence_index}/{run_number}.h5',
            'sequence': asdatetime(engaged),
            'sequence_index': sequence_index,
            'labscript': 'experiment.py',
            'run time': asdatetime(engaged) + pandas.Timedelta(seconds=run_number + 1),
            'run number': run_number,
            'run repeat': 0,
        }))
        for run_number in range(2)
    ]


class MostRecentSequencesTests(unittest.TestCase):

    def test_the_most_recently_engaged_sequences_are_returned(self):
        today_9 = a_sequence('20260923T101500', 9)
        today_10 = a_sequence('20260923T101500', 10)
        # Loaded after today's run, and with a higher sequence_index:
        last_week = a_sequence('20260916T101500', 30)
        # From before shots had a sequence_index:
        last_year = a_sequence('20250916T101500', None)
        df = concat_with_padding(*today_10, *today_9, *last_week, *last_year)
        server = object.__new__(WebServer)
        server.app = types.SimpleNamespace(
            logger=logging.getLogger('test'),
            filebox=types.SimpleNamespace(shots_model=types.SimpleNamespace(dataframe=df)),
        )
        for integer_indexing in (False, True):
            with self.subTest(integer_indexing=integer_indexing), mock.patch.object(
                dataframe_utilities.LABCONFIG, 'getboolean', return_value=integer_indexing
            ), mock.patch.object(lyse, 'zmq_get', lambda port, host, command, timeout:
                                 server.handler(command)):
                self.assertCountEqual(
                    lyse.data(n_sequences=1)['filepath'],
                    [shot['filepath'].iloc[0] for shot in today_10])
                self.assertCountEqual(
                    lyse.data(n_sequences=2)['filepath'],
                    [shot['filepath'].iloc[0] for shot in today_9 + today_10])


if __name__ == '__main__':
    unittest.main()
