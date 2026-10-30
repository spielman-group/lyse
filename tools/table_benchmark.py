#####################################################################
#                                                                   #
# /tools/table_benchmark.py                                         #
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
"""Time the shots table's work with many shots, and the longest the GUI thread
stalls doing it. The numbers compare builds on one machine only.

    python tools/table_benchmark.py [--shots 4000 50000]
"""
import argparse
import os
import sys
import threading
import time
import types

os.environ.setdefault('QT_QPA_PLATFORM', 'offscreen')
os.environ.setdefault('LABSCRIPT_DISABLE_GRAPHICAL_EXCEPTHOOK', '1')

import numpy as np
import pandas
from qtutils import inmain
from qtutils.qt import QtCore, QtWidgets

import lyse.filebox

N_COLUMNS = 200
N_SINGLESHOT = 1000
# Acceptance, in seconds, on this machine:
STALL_LIMIT = 0.5
SINGLESHOT_STALL_LIMIT = 0.1
LARGE = 50000

qapplication = QtWidgets.QApplication.instance() or QtWidgets.QApplication(sys.argv)


def dataset(n_shots):
    """The dataframe rows of n_shots shots, each with N_COLUMNS results."""
    rng = np.random.default_rng(0)
    columns = {('filepath', ''): [f'/shots/{n:06d}.h5' for n in range(n_shots)],
               ('run number', ''): np.arange(n_shots)}
    for i in range(N_COLUMNS):
        columns[f'routine{i // 20}', f'r{i}'] = rng.random(n_shots)
    return pandas.DataFrame(columns)


def shots_table(df=None, done=False):
    """A shown shots table, holding df's shots if given."""
    app = types.SimpleNamespace(
        qapplication=qapplication,
        output_box=types.SimpleNamespace(output=lambda text, red=False: None),
        filebox=types.SimpleNamespace(add_shots_start_time=None,
                                      set_add_shots_progress=lambda *args: None),
    )
    view = QtWidgets.QTableView()
    view.resize(1200, 800)
    view.show()
    shots = lyse.filebox.DataFrameModel(app, view, types.SimpleNamespace(getboolean=lambda *args: False))
    if df is not None:
        shots.add_files(list(df['filepath']), df, done=done)
    qapplication.processEvents()
    return shots, view


def measure(work):
    """Run work in a thread, as lyse's analysis and incoming threads do, and return
    its duration and the longest gap between ticks of a 10 ms timer in this, the GUI
    thread, until one tick after work ends."""
    ticks = [time.perf_counter()]
    finished = []
    loop = QtCore.QEventLoop()

    def tick():
        ticks.append(time.perf_counter())
        if finished and ticks[-1] > finished[0]:
            loop.quit()

    def run():
        start = time.perf_counter()
        try:
            work()
        except Exception as e:
            errors.append(e)
        finished.append(time.perf_counter())
        durations.append(finished[0] - start)

    durations, errors = [], []
    timer = QtCore.QTimer()
    timer.setInterval(10)
    timer.timeout.connect(tick)
    timer.start()
    threading.Thread(target=run, daemon=True).start()
    loop.exec()
    timer.stop()
    if errors:
        raise errors[0]
    return durations[0], max(np.diff(ticks))


def add(df):
    shots, view = shots_table()

    def work():
        # In batches, as the incoming loop adds them:
        start = 0
        while start < len(df):
            batch = df.iloc[start:start + len(shots.dataframe) // 3 + 1].reset_index(drop=True)
            shots.add_files(list(batch['filepath']), batch)
            start += len(batch)
    result = measure(work)
    # A case that did nothing would look fast:
    assert len(shots.filepaths) == len(df)
    return result


def singleshot(df):
    shots, view = shots_table(df)

    def work():
        for i in range(N_SINGLESHOT):
            filepath = shots.get_first_incomplete()
            shots.update_rows({filepath: {('singleshot', f's{j}'): i + j for j in range(5)}})
            shots.set_status_percent(filepath, 100)
    result = measure(work)
    assert shots.get_first_incomplete() == df['filepath'][N_SINGLESHOT]
    return result


def multishot(df):
    shots, view = shots_table(df)
    reply = {filepath: {('multishot', 'mean'): n} for n, filepath in enumerate(df['filepath'])}
    result = measure(lambda: shots.update_rows(reply))
    assert shots.dataframe['multishot', 'mean'].notna().all()
    return result


def remove(df, rows):
    shots, view = shots_table(df)
    selection = QtCore.QItemSelection()
    for row in rows:
        selection.select(shots._model.index(row, 0), shots._model.index(row, 0))
    view.selectionModel().select(
        selection, QtCore.QItemSelectionModel.Select | QtCore.QItemSelectionModel.Rows)
    result = measure(lambda: inmain(shots.remove_selection, confirm=False))
    assert len(shots.filepaths) == len(df) - len(rows)
    return result


def mark_not_done(df):
    shots, view = shots_table(df, done=True)
    view.selectAll()
    result = measure(lambda: inmain(shots.mark_selection_not_done))
    assert not any(shots.status_percent)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--shots', type=int, nargs='+', default=[4000, LARGE])
    args = parser.parse_args()
    passed = True
    print(f'{"case":<34}{"shots":>8}{"time (s)":>10}{"stall (s)":>11}')
    for n_shots in args.shots:
        df = dataset(n_shots)
        cases = {
            'adding the shots': lambda: add(df),
            f'single-shot analysis of {N_SINGLESHOT}': lambda: singleshot(df),
            'multishot reply to every row': lambda: multishot(df),
            'removing a contiguous half': lambda: remove(df, range(n_shots // 2)),
            'removing every other row': lambda: remove(df, range(0, n_shots, 2)),
            'removing 200 single rows': lambda: remove(df, range(0, 400, 2)),
            'marking all not done': lambda: mark_not_done(df),
        }
        for name, case in cases.items():
            duration, stall = case()
            limit = SINGLESHOT_STALL_LIMIT if name.startswith('single') else STALL_LIMIT
            failed = stall > limit and (n_shots >= LARGE or name.startswith('single'))
            passed &= not failed
            print(f'{name:<34}{n_shots:>8}{duration:>10.3f}{stall:>11.3f}{"  over limit" if failed else ""}')
    return 0 if passed else 1


if __name__ == '__main__':
    sys.exit(main())
