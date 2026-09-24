"""Run: saving results to the shot file and to the shot's row in lyse's
dataframe, opening the file, and so taking its lock, only to write to it."""
import contextlib
import os
import tempfile
import unittest

import lyse
import lyse.utils.worker

# labscript_utils.h5_lock, which lyse imports above, must be imported before
# h5py.
import h5py


@contextlib.contextmanager
def counting_opens():
    """Record the mode of every open of an hdf5 file, each of which takes the
    file's lock."""
    opens = []
    original = h5py.File

    class File(original):
        def __init__(self, name, mode='r', *args, **kwargs):
            opens.append(mode)
            super().__init__(name, mode, *args, **kwargs)

    h5py.File = File
    try:
        yield opens
    finally:
        h5py.File = original


class RunTests(unittest.TestCase):

    def setUp(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        # A shot as compiled, before any result is saved to it:
        self.path = os.path.join(folder.name, 'shot.h5')
        with h5py.File(self.path, 'w') as f:
            f.create_group('globals')
        # As inside lyse, where the row is updated from what save_result records:
        saved = lyse.utils.worker.spinning_top, lyse.utils.worker._updated_data
        lyse.utils.worker.spinning_top, lyse.utils.worker._updated_data = True, {}
        self.addCleanup(setattr, lyse.utils.worker, 'spinning_top', saved[0])
        self.addCleanup(setattr, lyse.utils.worker, '_updated_data', saved[1])

    def row(self):
        return lyse.utils.worker._updated_data[self.path]

    def test_a_run_saving_only_to_the_dataframe_never_opens_the_file(self):
        with counting_opens() as opens:
            run = lyse.Run(self.path)
            run.set_group('routine')
            run.save_result('N', 3, save_to_h5=False)
            run.save_results('width', 1.5, save_to_h5=False)
            run.save_results_dict({'height': 2.5}, save_to_h5=False)
        self.assertEqual(opens, [])
        self.assertEqual(self.row(), {('routine', 'N'): 3, ('routine', 'width'): 1.5,
                                      ('routine', 'height'): 2.5})

    def test_a_first_write_creates_the_group_and_writes_in_one_open(self):
        with counting_opens() as opens:
            run = lyse.Run(self.path)
            run.set_group('routine')
            run.save_result('N', 3)
        self.assertEqual(opens, ['r+'])
        self.assertEqual(self.row(), {('routine', 'N'): 3})
        with h5py.File(self.path, 'r') as f:
            self.assertEqual(f['results/routine'].attrs['N'], 3)

    def test_a_result_not_yet_saved_is_reported_as_before(self):
        with self.assertRaisesRegex(Exception, "result group 'routine' does not exist"):
            lyse.Run(self.path).get_result('routine', 'N')


if __name__ == '__main__':
    unittest.main()
