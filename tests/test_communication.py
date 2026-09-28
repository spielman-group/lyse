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
"""lyse.data(n_sequences=N) returns the N most recently engaged sequences,
lyse.data(where={column: value}) the rows whose columns match, and
LyseClient.add_shot queues a shot, each through a real lyse server."""
import concurrent.futures
import os
import queue
import types
import unittest
from unittest import mock

import numpy
import pandas
from qtutils.qt import QtWidgets

import lyse
from labscript_utils import shared_drive
from lyse import dataframe_utilities
from lyse.client import LyseClient
from lyse.communication import LyseServer
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


# The server copies lyse's dataframe in the main thread, through this
# application's events:
qapplication = QtWidgets.QApplication.instance() or QtWidgets.QApplication(['test'])


def a_server(df):
    """A lyse server on a free port, reading df and its queue of incoming shots
    from a stand-in for the application."""
    return LyseServer(types.SimpleNamespace(filebox=types.SimpleNamespace(
        shots_model=types.SimpleNamespace(dataframe=df), incoming_queue=queue.Queue())),
        bind_address='tcp://127.0.0.1')


def in_thread(f, **kwargs):
    """Return f(**kwargs), called in a thread while this one, the main thread,
    processes the events by which the server works in it."""
    with concurrent.futures.ThreadPoolExecutor(1) as pool:
        call = pool.submit(f, **kwargs)
        while concurrent.futures.wait([call], timeout=0.001).not_done:
            qapplication.processEvents()
    return call.result()


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
        self.addCleanup(server.shutdown)
        # lyse's integer_indexing setting decides the order it holds rows in,
        # which the answer must not depend on:
        for integer_indexing in (False, True):
            with self.subTest(integer_indexing=integer_indexing), mock.patch.object(
                dataframe_utilities.LABCONFIG, 'getboolean', return_value=integer_indexing
            ):
                self.assertCountEqual(
                    in_thread(lyse.data, port=server.port, n_sequences=1)['filepath'],
                    [shot['filepath'].iloc[0] for shot in today_10])
                self.assertCountEqual(
                    in_thread(lyse.data, port=server.port, n_sequences=2)['filepath'],
                    [shot['filepath'].iloc[0] for shot in today_9 + today_10])


class WhereTests(unittest.TestCase):

    def setUp(self):
        self.older = a_sequence('20260923T091500', 0)
        self.newer = a_sequence('20260923T101500', 1)
        self.server = a_server(concat_with_padding(*self.older, *self.newer))
        self.addCleanup(self.server.shutdown)

    def data(self, **kwargs):
        with mock.patch.object(dataframe_utilities.LABCONFIG, 'getboolean', return_value=False):
            return in_thread(lyse.data, port=self.server.port, **kwargs)

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
        with self.assertRaisesRegex(ValueError, 'no_such_column'):
            self.data(where={'no_such_column': 1})

    def test_rows_are_chosen_after_n_sequences(self):
        """A row of an older sequence is not among the most recent one's."""
        older_shot = self.older[0]['filepath'].iloc[0]
        self.assertEqual(len(self.data(n_sequences=1, where={'filepath': older_shot})), 0)


class AddShotTests(unittest.TestCase):

    def test_a_submitted_shot_reaches_the_incoming_queue_as_a_local_path(self):
        server = a_server(None)
        self.addCleanup(server.shutdown)
        path = os.path.join(shared_drive.prefix, 'shot.h5')
        LyseClient('localhost', server.port, 5).add_shot(shared_drive.path_to_agnostic(path))
        self.assertEqual(server.app.filebox.incoming_queue.get(timeout=5), path)


if __name__ == '__main__':
    unittest.main()
