"""The FileBox's dataframe, recording the results a routine hands back."""
import types
import unittest
import warnings

with warnings.catch_warnings():
    warnings.simplefilter('ignore')
    import lyse.filebox


class ResultsForARemovedShotTests(unittest.TestCase):

    def test_results_for_a_shot_with_no_row_are_reported_and_the_list_stays_live(self):
        """A shot removed while its routine ran has no row to take its results,
        and a result not saved to the file is kept nowhere else."""
        output, signals_blocked = [], []
        model = types.SimpleNamespace(
            row_number_by_filepath={},
            app=types.SimpleNamespace(output_box=types.SimpleNamespace(
                output=lambda text, red=False: output.append(text))),
            _model=types.SimpleNamespace(blockSignals=signals_blocked.append),
        )
        lyse.filebox.DataFrameModel.update_row(
            model, 'removed.h5', updated_row_data={('routine', 'N'): 3})
        self.assertTrue(any('removed.h5' in text for text in output))
        # Left blocked, the model would stop telling the view about new rows:
        self.assertNotEqual(signals_blocked[-1:], [True])


if __name__ == '__main__':
    unittest.main()
