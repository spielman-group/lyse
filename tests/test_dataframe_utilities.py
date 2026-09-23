"""Replacing one shot's row in the FileBox dataframe.

``replace_with_padding`` swaps the row at a given index for a freshly built one,
padding whichever side has the shallower column hierarchy so that the two can be
combined. The frames here are built the way lyse builds them: a nested dict of a
shot's attributes, flattened, made hierarchical, and concatenated with padding
onto the rows before it.
"""
import unittest

from lyse.dataframe_utilities import (
    concat_with_padding,
    flat_dict_to_hierarchical_dataframe,
    flatten_dict,
    replace_with_padding,
)


def a_row(nested):
    return flat_dict_to_hierarchical_dataframe(flatten_dict(nested))


class ReplaceWithPaddingTests(unittest.TestCase):

    def setUp(self):
        self.df = concat_with_padding(
            a_row({'filepath': 'a.h5', 'x': 1}),
            a_row({'filepath': 'b.h5', 'x': 2}),
            a_row({'filepath': 'c.h5', 'x': 3}),
        )

    def test_the_row_at_the_index_is_replaced_in_place(self):
        df = replace_with_padding(self.df, a_row({'filepath': 'b.h5', 'x': 20}), 1)
        self.assertEqual(['a.h5', 'b.h5', 'c.h5'], list(df['filepath']))
        self.assertEqual([1, 20, 3], list(df['x']))
        self.assertEqual([0, 1, 2], list(df.index))

    def test_a_row_with_deeper_columns_pads_the_frame(self):
        row = a_row({'filepath': 'b.h5', 'x': 2, 'results': {'routine': {'n': 5}}})
        df = replace_with_padding(self.df, row, 1)
        self.assertEqual(3, df.columns.nlevels)
        self.assertEqual(5, df['results', 'routine', 'n'][1])
        self.assertEqual(['a.h5', 'b.h5', 'c.h5'], list(df['filepath']))

    def test_a_row_with_shallower_columns_is_padded_to_the_frame(self):
        deep_row = a_row({'filepath': 'b.h5', 'x': 2, 'results': {'routine': {'n': 5}}})
        deep = replace_with_padding(self.df, deep_row, 1)
        df = replace_with_padding(deep, a_row({'filepath': 'c.h5', 'x': 30}), 2)
        self.assertEqual(3, df.columns.nlevels)
        self.assertEqual([1, 2, 30], list(df['x']))
