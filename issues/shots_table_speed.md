# Shots table speed

Upstream's large-table set, to be ported onto `CodeHealth` for Ian's review:
4d30a1e, 10d9022, 79f9db9, b33f1c6, 8f4d140, 0bb3acd, ed71cc7, 4954e44,
4edeed4, 4394eb3. (f347bca, the add-shots dialog's sort, is already ported.)

## Requirements

1. With tens of thousands of shots, adding, removing, analysing and drawing
   shots stays responsive: no Qt item per cell, no relayout of the whole table
   per routine, no removal or duplicate check that grows with the square of the
   shots.
2. The table shows what the dataframe holds, read as it is drawn: values,
   tooltips, the status column's progress bar, the deleted-off-disk icon and the
   integer-indexing row labels, formatted as now.
3. The dataframe stays the one store. `lyse.data()`, `where=`, `n_sequences`,
   `columns=` and `shot_id` behave as now, since the server reads only it.
4. A routine's reply reaches the dataframe in one main-thread call, for all its
   rows. The row it leaves equals a reload of the shot: dicts flattened into
   column levels, the pandas 3 dtype rules (a float makes an integer column
   float64, anything else incompatible makes it object).
5. Results for a shot with no row are reported in the output box, as now.
6. `lyse.paths` is unchanged: the filebox's `analysed_since_multishot`, a failed
   pass's shots offered to the next.
7. Columns behave as now: visibility, the edit-columns dialog, new and defunct
   columns, added levels.
8. Status behaves as now: the first shot not analysed, mark as not done, the
   progress percentage, deleted off disk, pause and Abort.
9. A routine box makes two main-thread calls per routine run, not seven.
10. The add-shots progress bar shows only once adding has taken 0.5 s.

## Architecture

- `ShotsTableModel(QAbstractTableModel)`, upstream's, replaces the
  `QStandardItemModel` of the shots table. Its rows come from three
  `DataFrameModel` lists, `filepaths`, `status_percent` and `deleted_off_disk`;
  its columns from `column_names`. `data()` reads `dataframe.iat` through a
  cache of column positions, which `dataframe_changed()` clears whenever the
  dataframe is replaced or its columns renumbered. `headerData()` gives the
  column names and `row_label()`. labscript-utils' `HorizontalHeaderViewWithWidgets`
  serves it as it is: it reads header items only for sections holding a widget,
  and the shots table's header holds none.
- `DataFrameModel` keeps the surface the filebox and server use: `dataframe`,
  `row_number_by_filepath`, `add_files`, `set_status_percent`,
  `get_first_incomplete`, `mark_as_deleted_off_disk`, `mark_selection_not_done`,
  `remove_selection`, `set_columns_visible`, `update_column_levels`.
  `update_row` gives way to upstream's `update_rows(updated_data)`, which
  takes a whole reply. It keeps the fork's report of a shot with no row and the
  fork's dtype fallback. A column with one updated row is set with `.at`, more
  rows or a new column with `.loc`. It emits `dataChanged` for the cells it
  touched, and `layoutChanged` only when it added columns.
- `FileBox` calls `update_rows` once per reply, and skips a progress update that
  changes nothing. `analysed_since_multishot` is untouched.
- Removal reads the selection's ranges and removes each block of rows with one
  `removeRows`, last block first. The routine lists and the edit-columns dialog
  use a plain `QStandardItemModel` with `NoEditTriggers` in place of
  `UneditableModel`, whose Python `flags()` Qt called per cell.
- The first row not analysed is kept, and moves back when shots are added,
  removed or marked not done, or a status below 100 is set.
- `add_files` checks duplicates with a set. The progress bar shows from a 0.5 s
  timer, or from the next progress update while the main thread is busy.
- `RoutineBox.do_analysis` (4394eb3) makes the status changes before and after
  each routine in one main-thread call each, as upstream's `start_analysis` and
  `end_routine`. These must carry the fork's `(filepath, paths)` arguments,
  Abort's `working` and `run_number`, the multishot rule of 6003d7c, and the
  fork's `stopped` value in place of 9c52248's `done`. `todo()` goes.

## Size and risks

- About +400 −350 lines in `filebox.py`, `routines.py` and `widgets.py`, as
  upstream's.
- The fork's tests stub `update_row` and the shots model; they move to
  `update_rows`. `tests/test_widgets.py` paints the delegate over a
  `QStandardItemModel`; it moves to `ShotsTableModel`.
- One new test: a result saved by a routine shows in the table, in a new
  column, read from the dataframe.
