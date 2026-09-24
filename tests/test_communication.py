#####################################################################
#                                                                   #
# /tests/test_communication.py                                      #
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
"""lyse.data(n_sequences=N) returns the N most recently engaged sequences, and
lyse.data(where={column: value}) the rows whose columns match."""
import logging
import types
import unittest
from unittest import mock

import numpy
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
            'fit': {'atoms': 100 + run_number},
        }))
        for run_number in range(2)
    ]


def a_server(df):
    """The lyse server, holding df, without the port its constructor binds."""
    server = object.__new__(WebServer)
    server.app = types.SimpleNamespace(
        logger=logging.getLogger('test'),
        filebox=types.SimpleNamespace(shots_model=types.SimpleNamespace(dataframe=df)),
    )
    return server


class MostRecentSequencesTests(unittest.TestCase):

    def test_the_most_recently_engaged_sequences_are_returned(self):
        today_9 = a_sequence('20260923T101500', 9)
        today_10 = a_sequence('20260923T101500', 10)
        # Loaded after today's run, and with a higher sequence_index:
        last_week = a_sequence('20260916T101500', 30)
        # From before shots had a sequence_index:
        last_year = a_sequence('20250916T101500', None)
        df = concat_with_padding(*today_10, *today_9, *last_week, *last_year)
        server = a_server(df)
        # lyse's integer_indexing setting decides the order it holds rows in,
        # which the answer must not depend on:
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


class WhereTests(unittest.TestCase):

    def setUp(self):
        self.older = a_sequence('20260923T091500', 0)
        self.newer = a_sequence('20260923T101500', 1)
        self.server = a_server(concat_with_padding(*self.older, *self.newer))

    def data(self, **kwargs):
        with mock.patch.object(dataframe_utilities.LABCONFIG, 'getboolean', return_value=False), \
             mock.patch.object(lyse, 'zmq_get', lambda port, host, command, timeout:
                               self.server.handler(command)):
            return lyse.data(**kwargs)

    def test_rows_are_chosen_by_a_list_of_filepaths(self):
        wanted = [self.older[1]['filepath'].iloc[0], self.newer[0]['filepath'].iloc[0]]
        for kind in (list, numpy.array, frozenset):
            with self.subTest(kind.__name__):
                self.assertCountEqual(self.data(where={'filepath': kind(wanted)})['filepath'], wanted)

    def test_rows_are_chosen_by_equality_on_a_nested_column(self):
        rows = self.data(where={('fit', 'atoms'): 101})
        self.assertCountEqual(rows['filepath'],
                              [self.older[1]['filepath'].iloc[0], self.newer[1]['filepath'].iloc[0]])

    def test_a_column_not_in_the_dataframe_is_refused_by_name(self):
        with self.assertRaisesRegex(KeyError, 'no_such_column'):
            self.data(where={'no_such_column': 1})

    def test_rows_are_chosen_after_n_sequences(self):
        """A row of an older sequence is not among the most recent one's."""
        older_shot = self.older[0]['filepath'].iloc[0]
        self.assertEqual(len(self.data(n_sequences=1, where={'filepath': older_shot})), 0)

    def test_a_request_with_something_else_in_that_place_is_refused(self):
        """A client still passing n_shots sends an integer there."""
        reply = self.server.handler(('get dataframe', None, None, 1))
        self.assertTrue(reply.startswith('error: operation not supported'))


if __name__ == '__main__':
    unittest.main()
