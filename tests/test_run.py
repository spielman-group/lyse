"""Run.save_result, which saves a result to the shot file and to the shot's row
in lyse's dataframe, or with save_to_h5=False to the row alone."""
import os
import tempfile
import unittest

import lyse
import lyse.utils.worker

# labscript_utils.h5_lock, which lyse imports above, must be imported before
# h5py.
import h5py


class SaveResultTests(unittest.TestCase):

    def setUp(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.path = os.path.join(folder.name, 'shot.h5')
        with h5py.File(self.path, 'w') as f:
            f.create_group('globals')
        # As inside lyse, where the row is updated from what save_result records:
        saved = lyse.utils.worker.spinning_top, lyse.utils.worker._updated_data
        lyse.utils.worker.spinning_top, lyse.utils.worker._updated_data = True, {}
        self.addCleanup(setattr, lyse.utils.worker, 'spinning_top', saved[0])
        self.addCleanup(setattr, lyse.utils.worker, '_updated_data', saved[1])
        self.run_ = lyse.Run(self.path)
        self.run_.set_group('routine')

    def row(self):
        return lyse.utils.worker._updated_data[self.path]

    def attributes(self):
        with h5py.File(self.path, 'r') as f:
            return dict(f['results/routine'].attrs)

    def test_a_result_not_saved_to_h5_updates_the_row_without_opening_the_file(self):
        os.rename(self.path, self.path + '.away')
        self.run_.save_result('N', 3, save_to_h5=False)
        os.rename(self.path + '.away', self.path)
        self.assertEqual(self.row()[('routine', 'N')], 3)
        self.assertNotIn('N', self.attributes())

    def test_by_default_a_result_is_saved_to_the_file_and_the_row(self):
        self.run_.save_result('N', 3)
        self.assertEqual(self.row()[('routine', 'N')], 3)
        self.assertEqual(self.attributes()['N'], 3)


if __name__ == '__main__':
    unittest.main()
