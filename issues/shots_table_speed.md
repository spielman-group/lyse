# Shots table speed

Upstream's large-table set, built from upstream's own commits: 4d30a1e, 10d9022,
79f9db9, b33f1c6, 8f4d140, 0bb3acd, ed71cc7, 4954e44, 4edeed4, 4394eb3.
(f347bca, the add-shots dialog's sort, is already in, as 3d866a1.)

## Requirements

1. With tens of thousands of shots, adding, removing, analysing and drawing
   shots stays responsive, as the benchmark below measures.
2. The code is upstream's wherever the fork does not truly differ.
3. These behave as now: selecting rows; no cell is editable; column visibility,
   kept for a column removed and added again; column order, status and filepath
   first and new columns appended; Save dataframe and Load dataframe; Mark as
   not done, Run multishot analysis, pause and Abort.
4. `lyse.data()`, `where=`, `n_sequences`, `columns=` and `shot_id` behave as
   now: the server reads only the dataframe, which stays the one store.
5. `lyse.paths` behaves as now: a failed pass's shots are offered to the next.
6. Results for a shot with no row are reported in the output box, as Ian kept.
7. At every row-removal notification, the model's lists and the dataframe
   describe the same rows, since the view reads the dataframe as it draws.

Out of scope: sorting or filtering the shot rows, which no model does now. The
add-shots dialog's natural sort orders files as they are added, EditColumns'
filter narrows its own column list, and `where=` chooses rows for a client only;
none of them changes the table's rows.

## Build

On `CodeHealth`, `git cherry-pick -x` each commit below, in the order upstream
merged them into `master`. Every conflict is resolved to upstream's side, except
the adaptations below, each its own commit with its reason. Starred commits are
upstream commits the fork already ported in its own form into code that the set
replaces; they are picked again so that the code is upstream's. A dry run of
this sequence, in a scratch clone, gave the conflicts below. The suite then
failed only where the tests stub `update_row` and call `todo()`; D and E were
needed though no test reached them, since the dry run called `lyse.utils.gui`'s
dialogs, which the fork no longer has, and opened a GUI routine's folder.

| Upstream | How it goes in |
| --- | --- |
| ed71cc7 | As is. Its conflicts are neighbouring fork lines (d2bc831's header set-up), kept beside it. |
| 0bb3acd | As is, but adaptation D. |
| 79f9db9 | As is, then adaptation A. |
| 061ce1f* | Picked again, clean. |
| b33f1c6 | As is, clean. |
| 8f4d140 | As is, clean. |
| 4394eb3 | As is, then adaptation C. |
| b35294c* | Picked again, clean. |
| 4d30a1e | As is, then fix F. Its twelve conflicts are the style pass and the fork's forms of upstream commits merged after it (1beb61c's icons, 3a0888c's header fonts, 2b5d616's row height, 74e6127's file opening), each picked again in its place below. |
| 10d9022 | As is, clean. |
| 4edeed4 | As is. One conflict, an f-string. |
| 4954e44 | As is. One conflict, an f-string. |
| 1beb61c* | Picked again. Its two conflicts are the fork's own `Icon` lines. |
| 6003d7c* | Picked again onto 4394eb3's `start_analysis` and `end_routine`. Its conflicts are the fork's port of it, which goes. |
| 9c52248* | Picked again, clean, then adaptation B. |
| 3a0888c* | Picked again. Its one conflict, in `__main__.py`, keeps the fork's import, which already has `setup_application`. |
| 2b5d616* | Picked again, clean. It removes the per-row resize that 4d30a1e adds, so it comes after it. |
| 74e6127* | Picked again, then adaptations D and E. |

### Adaptations and fixes

- **A, results for a removed shot.** 79f9db9's `update_rows` skips a reply's
  shot that has no row, silently. The fork reports it, since a result saved with
  `save_to_h5=False` is then kept nowhere (Ian kept this, as candidate B of
  step 2.1). Three lines, after 79f9db9 unchanged; offered upstream, and replaced
  by upstream's commit once it is accepted.
- **B, `lyse.paths` after a failed multishot pass.** Upstream's 9c52248 sends
  `done` when the routines after a failed one ran, and the filebox then forgets
  the pass's shots. The fork sends `error`, with 100 rather than None as its
  status, so that `do_multishot_analysis` puts the shots back for the next pass,
  and the pause state is still decided in the routine box.
- **C, `lyse.paths` itself.** `RoutineBox.do_analysis` and
  `AnalysisRoutine.do_analysis` take `(filepath, paths)`, a multishot box's
  shots since its last pass; upstream's take `filepath` only.
- **D, the app's dialogs.** 0bb3acd's removal prompt and 74e6127's viewer and
  editor error handlers call `lyse.utils.gui.question_dialog` and
  `error_dialog`, which step 2.2 replaced with the app's own, from
  labscript-utils' `LabscriptGuiApplication`: `self.app.question_dialog` and
  `self.app.error_dialog`.
- **E, a GUI routine's code.** 74e6127 opens a routine's path in the editor; a
  GUI routine's path is its `.lyse` folder, which the fork opens as its
  `lyse_routine.py`, as now.
- **F, removal notifications.** 4d30a1e's `remove_selection` drops the selected
  rows from the dataframe and renumbers it before it sends the first block's
  removal, so while the blocks' notifications run, the model's lists and the
  dataframe disagree, and a redraw reads the wrong shot's data or past the end.
  Each block, last first, instead leaves the dataframe and the lists together
  between that block's `beginRemoveRows` and `endRemoveRows`. A selection of
  more than a few hundred blocks is removed in one model reset instead, one
  drop between `beginResetModel` and `endResetModel`, so that removal stays
  linear in the shots; removal clears the selection either way. The cutoff is a
  class constant, 200 blocks. Its own commit, offered upstream.
- The tests that stub `update_row` and call `todo()` move to `update_rows` and
  `start_analysis`/`end_routine`.

### Upstream

- A and F are bug fixes, offered upstream on `UpstreamBugFixes`.
- `lyse.paths` is used: labscript-optimization's and labscript-agents'
  routines read `self.paths`. So, by Ian's rule, the whole of it, the paths a
  multishot routine is given, their retry after a failed pass, and the routine
  box and worker changes that carry them, is offered upstream. B and C then
  become upstream's. The Upstream helper session has it.
- History: 79f9db9 goes in unchanged, then A as a separate commit; once
  upstream accepts A, its commit replaces the fork's. Taking upstream's
  `update_rows` removes all the fork's own `update_row` held except A: the rest
  were ports of upstream commits that upstream's code holds (0b010a9, 00a26a1,
  3723c9b's signals) or that are picked again (061ce1f, b35294c).

## Tests

Offscreen, on the real `DataFrameModel` and its `ShotsTableModel`, as the
behaviours users rely on (rule 13); each replaces a stub test or adds one:

1. Removal, with the cutoff set low for the test: one block, two blocks below
   the cutoff, and more blocks than it. At each removal notification the table's
   filepath column matches the dataframe's. Afterwards the surviving shots and
   their statuses are right, a reply updates the right rows, the next shot not
   analysed is the right one, and column visibility is kept.
2. A reply that adds a column shows it in the header; a hidden column stays
   hidden when columns are added, and when it is removed and added again.
3. Save dataframe then Load dataframe gives the same rows and values.
4. A failed multishot pass: the routines after it run, and the next pass's
   `lyse.paths` holds the failed pass's shots (the existing `lyse.paths` tests,
   on the real routine box).
5. The same shot submitted twice, in one batch and in two, gives one row.
6. `lyse.data()` from a client thread while shots are added and removed returns
   whole, consistent frames.

## Benchmark

A script in the repo, `tools/table_benchmark.py`, run offscreen with the shots
table shown. Each case starts from a dataset the script makes: 4,000 or 50,000
shots, each with 200 result columns. It times:

- adding the shots;
- on a preloaded table, single-shot analysis of 1,000 shots in turn, each a
  one-row reply and a lookup of the next shot not analysed;
- a multishot reply updating every row;
- removing a contiguous half, every other row, and a selection one block below
  F's cutoff;
- marking all not done.

For each case it reports the time and the longest stall of the GUI thread: the
largest gap between ticks of a 10 ms `QTimer`, sampled until one tick after the
case ends, so that a stall at its end counts. Acceptance, on this machine: no
stall longer than 0.5 s in any case with 50,000 shots, and none longer than
0.1 s in single-shot analysis. Numbers compare builds on one machine only.

## Size

About +400 −350 lines in `filebox.py`, `routines.py` and `widgets.py`, nearly
all upstream's; the adaptations and F about 60 lines together.
