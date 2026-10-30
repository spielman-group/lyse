#####################################################################
#                                                                   #
# /tests/test_filebox.py                                            #
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
"""The shots table: shots added, removed and updated, as the view and
lyse.data() read them from the one dataframe."""
import concurrent.futures
import socket
import types
import unittest

import pandas
from qtutils.qt import QtCore, QtWidgets

import lyse
import lyse.filebox
from lyse.communication import LyseServer

qapplication = QtWidgets.QApplication.instance() or QtWidgets.QApplication(['test'])


def rows(numbers, x=True):
    """The dataframe rows of the shots with these run numbers."""
    columns = {('filepath', ''): [f'{n}.h5' for n in numbers], ('run number', ''): list(numbers)}
    if x:
        columns['x', ''] = [float(n) for n in numbers]
    return pandas.DataFrame(columns)


class ShotsTableTests(unittest.TestCase):

    def shots_table(self, numbers):
        self.output = []
        app = types.SimpleNamespace(
            qapplication=qapplication,
            output_box=types.SimpleNamespace(output=lambda text, red=False: self.output.append(text)),
            filebox=types.SimpleNamespace(add_shots_start_time=None, fire_event=lambda *args: None,
                                          set_add_shots_progress=lambda *args: None),
        )
        self.view = QtWidgets.QTableView()
        self.view.show()
        self.addCleanup(self.view.hide)
        exp_config = types.SimpleNamespace(getboolean=lambda *args: False)
        shots = lyse.filebox.DataFrameModel(app, self.view, exp_config)
        shots.add_files([f'{n}.h5' for n in numbers], rows(numbers), ['remote'] * len(numbers))
        return shots

    def remove(self, shots, numbers):
        selection = self.view.selectionModel()
        for n in numbers:
            selection.select(shots._model.index(shots.row_number_by_filepath[f'{n}.h5'], 0),
                             QtCore.QItemSelectionModel.Select | QtCore.QItemSelectionModel.Rows)
        shots.remove_selection(confirm=False)

    def test_removed_shots_leave_the_rest_whole(self):
        # One block, removed as rows, and two, removed with a model reset:
        for removed in [{2, 3, 4}, {1, 4, 5}]:
            with self.subTest(removed=removed):
                shots = self.shots_table(range(8))
                done = {0, 1, 2, 3, 4, 6}
                for n in done:
                    shots.end_attempt(f'{n}.h5', 'done')
                # Analysis has moved past the rows to be removed:
                self.assertEqual(shots.get_first_incomplete(), '5.h5')
                x = shots.column_indices['x', '']
                shots.set_columns_visible({**shots.columns_visible, x: False})
                checks = []

                def check(*args):
                    model = shots._model
                    try:
                        column = [model.data(model.index(row, shots.COL_FILEPATH))
                                  for row in range(model.rowCount())]
                    except IndexError:
                        column = None
                    checks.append(column == shots.filepaths)
                for signal in [shots._model.rowsAboutToBeRemoved, shots._model.rowsRemoved,
                               shots._model.modelAboutToBeReset, shots._model.modelReset]:
                    signal.connect(check)
                self.remove(shots, removed)

                kept = [n for n in range(8) if n not in removed]
                self.assertTrue(checks and all(checks))
                self.assertEqual(shots.filepaths, [f'{n}.h5' for n in kept])
                self.assertEqual(shots.states, ['done' if n in done else 'not done' for n in kept])
                self.assertEqual(shots.get_first_incomplete(), f'{min(set(kept) - done)}.h5')
                shots.update_rows({'7.h5': {('fit', 'y'): 'seven'}})
                index = shots._model.index(len(kept) - 1, shots.column_indices['fit', 'y'])
                self.assertEqual(shots._model.data(index), 'seven')
                self.assertTrue(self.view.isColumnHidden(x))
                # A result for a removed shot, if not saved to its file, is kept nowhere:
                shots.update_rows({f'{min(removed)}.h5': {('fit', 'y'): 'lost'}})
                self.assertTrue(any(f'{min(removed)}.h5' in text for text in self.output))

    def test_a_hidden_column_stays_hidden_as_columns_come_and_go(self):
        shots = self.shots_table(range(3))
        shots.set_columns_visible({**shots.columns_visible, shots.column_indices['x', '']: False})
        shots.update_rows({'0.h5': {('fit', 'y'): 1}})
        y = shots.column_indices['fit', 'y']
        self.assertEqual(shots._model.headerData(y, QtCore.Qt.Horizontal), 'fit\ny')
        self.assertTrue(self.view.isColumnHidden(shots.column_indices['x', '']))
        # Once every shot is gone, shots without x remove its column:
        self.remove(shots, range(3))
        shots.add_files(['3.h5'], rows([3], x=False), ['remote'])
        shots.add_files(['4.h5'], rows([4]), ['remote'])
        self.assertTrue(self.view.isColumnHidden(shots.column_indices['x', '']))

    def test_a_shot_submitted_again_has_one_row(self):
        # The incoming loop drops a batch's own duplicates before adding it.
        shots = self.shots_table([0])
        shots.add_files(['0.h5'], rows([0]), ['remote'])
        self.assertEqual(shots.filepaths, ['0.h5'])
        self.assertEqual(len(shots.dataframe), 1)

    def test_lyse_data_returns_whole_frames_while_shots_come_and_go(self):
        shots = self.shots_table(range(8))
        with socket.socket() as probe:
            probe.bind(('127.0.0.1', 0))
            port = probe.getsockname()[1]
        server = LyseServer(types.SimpleNamespace(filebox=types.SimpleNamespace(shots_model=shots)),
                            port, bind_address='tcp://127.0.0.1')
        self.addCleanup(server.shutdown)
        with concurrent.futures.ThreadPoolExecutor(1) as pool:
            call = pool.submit(lambda: [lyse.data(port=port) for _ in range(20)])
            n = 8
            while not call.done():
                shots.add_files([f'{n}.h5'], rows([n]), ['remote'])
                self.remove(shots, [n - 8])
                n += 1
                qapplication.processEvents()
        for frame in call.result():
            self.assertEqual(len(frame), 8)
            self.assertEqual(list(frame['x']), list(frame['run number']))
            self.assertEqual(list(frame['filepath']), [f'{n}.h5' for n in frame['run number']])


if __name__ == '__main__':
    unittest.main()
