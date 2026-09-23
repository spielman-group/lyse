"""Choosing the most recent sequences for ``lyse.data(n_sequences=N)``.

``WebServer._extract_n_sequences_from_df`` keeps the rows of the N most recent
sequences, one sequence per call to engage in runmanager. Most recent means most
recently engaged, which is not the same as most recently loaded: last week's
files dragged into lyse after today's run are older, not newer.

The frames here are built the way lyse builds its own. Each shot's row comes
from the nested dict lyse reads out of a shot file, flattened and padded to a
common column depth, and the rows are concatenated onto the FileBox's empty
frame. The request handler then gives the frame its row index before asking for
the sequences, and so do these tests. That index is sorted, so the order the
rows arrive in depends on lyse's ``integer_indexing`` setting -- by engage time
without it, by ``sequence_index`` with it -- and every test runs under both.
"""
import unittest
from unittest import mock

import pandas

from lyse import dataframe_utilities
from lyse.communication import WebServer
from lyse.dataframe_utilities import (
    asdatetime,
    concat_with_padding,
    flat_dict_to_hierarchical_dataframe,
    flatten_dict,
    rangeindex_to_multiindex,
)

LAST_WEEK = '20260916T101500'
EARLIER_TODAY = '20260923T091500'
TODAY = '20260923T101500'


def a_sequence(engaged, sequence_index, labscript='experiment.py'):
    """The rows lyse makes for the two shots of one engage."""
    shots = []
    for run_number in range(2):
        shot = {
            'detuning': 1.5,
            'fit': {'amplitude': 0.3},
            'side': {'absorption': {'OD': {'exposure': 0.1}}},
            'filepath': f'{labscript}/{engaged}_{sequence_index}/{run_number}.h5',
            'sequence': asdatetime(engaged),
            'sequence_index': sequence_index,
            'labscript': labscript,
            'run time': asdatetime(engaged) + pandas.Timedelta(seconds=run_number + 1),
            'run number': run_number,
            'run repeat': 0,
            'n_runs': 2,
        }
        shots.append(flat_dict_to_hierarchical_dataframe(flatten_dict(shot)))
    return shots


def filepaths(*sequences):
    return [shot['filepath'].iloc[0] for sequence in sequences for shot in sequence]


class MostRecentSequencesTests(unittest.TestCase):
    integer_indexing = False

    def dataframe(self, *sequences):
        """The frame the request handler holds once these sequences are loaded
        into lyse, in the order given."""
        index = pandas.MultiIndex.from_tuples([('filepath', '')])
        df = pandas.DataFrame({'filepath': []}, columns=index)
        shots = [shot for sequence in sequences for shot in sequence]
        if shots:
            df = concat_with_padding(df, *shots)
        with mock.patch.object(
            dataframe_utilities.LABCONFIG, 'getboolean', return_value=self.integer_indexing
        ):
            return rangeindex_to_multiindex(df, inplace=True)

    def most_recent(self, n_sequences, *sequences):
        df = self.dataframe(*sequences)
        # The method uses nothing of the server, whose constructor binds a port.
        result = WebServer._extract_n_sequences_from_df(None, df, n_sequences)
        return list(result['filepath'])

    def test_engages_in_the_same_second_are_ordered_by_sequence_index(self):
        ninth = a_sequence(TODAY, 9)
        tenth = a_sequence(TODAY, 10)
        self.assertEqual(self.most_recent(1, ninth, tenth), filepaths(tenth))

    def test_files_loaded_after_todays_run_are_older(self):
        today = a_sequence(TODAY, 0)
        last_week = a_sequence(LAST_WEEK, 3)
        self.assertEqual(self.most_recent(1, today, last_week), filepaths(today))

    def test_two_most_recent_sequences_come_in_engage_order(self):
        earlier_today = a_sequence(EARLIER_TODAY, 0)
        today = a_sequence(TODAY, 1)
        last_week = a_sequence(LAST_WEEK, 3)
        self.assertEqual(
            self.most_recent(2, today, earlier_today, last_week),
            filepaths(earlier_today, today),
        )

    def test_shots_without_a_sequence_index_are_one_sequence_ordered_by_engage_time(self):
        today = a_sequence(TODAY, 0)
        last_week = a_sequence(LAST_WEEK, None)
        self.assertEqual(self.most_recent(1, today, last_week), filepaths(today))
        self.assertCountEqual(self.most_recent(2, today, last_week), filepaths(last_week, today))

    def test_a_missing_sequence_index_sorts_before_one_engaged_in_the_same_second(self):
        indexed = a_sequence(TODAY, 0)
        unindexed = a_sequence(TODAY, None)
        self.assertEqual(self.most_recent(1, indexed, unindexed), filepaths(indexed))

    def test_labscripts_sharing_a_sequence_index_in_the_same_second_are_two_sequences(self):
        first = a_sequence(TODAY, 0, labscript='a.py')
        second = a_sequence(TODAY, 0, labscript='b.py')
        for loaded, label in [((first, second), 'a.py first'), ((second, first), 'b.py first')]:
            with self.subTest(label):
                self.assertEqual(self.most_recent(1, *loaded), filepaths(second))
                self.assertCountEqual(self.most_recent(2, *loaded), filepaths(first, second))

    def test_an_empty_dataframe_is_returned_as_it_is(self):
        df = self.dataframe()
        self.assertIs(WebServer._extract_n_sequences_from_df(None, df, 1), df)

    def test_zero_sequences_is_no_rows(self):
        self.assertEqual(self.most_recent(0, a_sequence(TODAY, 0)), [])


class MostRecentSequencesUnderIntegerIndexingTests(MostRecentSequencesTests):
    """The same, with the rows arriving sorted by sequence_index."""
    integer_indexing = True


if __name__ == '__main__':
    unittest.main()
