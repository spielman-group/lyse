# Shots table speed

Upstream's large-table set, built from upstream's own commits: 4d30a1e, 10d9022,
79f9db9, b33f1c6, 8f4d140, 0bb3acd, ed71cc7, 4954e44, 4edeed4, 4394eb3.
(f347bca, the add-shots dialog's sort, is already in, as 3d866a1.)

## Requirements

1. With tens of thousands of shots, adding, removing, analysing and drawing
   shots stays responsive.
2. The code is upstream's wherever the fork does not truly differ.
3. `lyse.data()`, `where=`, `n_sequences`, `columns=` and `shot_id` behave as
   now: the server reads only the dataframe, which stays the one store.
4. `lyse.paths` behaves as now: a failed pass's shots are offered to the next.
5. Results for a shot with no row are reported in the output box, as Ian kept.

## Build

On `CodeHealth`, `git cherry-pick -x` each commit below, in upstream's order.
Every conflict is resolved to upstream's side, except the adaptations A, B and C,
each its own commit with its reason. Starred commits are upstream commits the
fork already ported in its own form into code that the set replaces; they are
picked again so that the code is upstream's. A dry run of the whole stack, in a
scratch clone, gave the conflicts below; the suite then failed only at A, B, C,
the test harness, and the dry run's own slip of taking 3a0888c's import block
from upstream, where the fork's already imports `setup_application`.

| Upstream | How it goes in |
| --- | --- |
| ed71cc7 | As is. Its conflicts are neighbouring fork lines (d2bc831's header set-up), kept beside it. |
| 0bb3acd | As is, but one line: the removal prompt calls the app's `question_dialog`, labscript-utils' shared helper from step 2.2, since `lyse.utils.gui.question_dialog` is gone. |
| 79f9db9 | As is: upstream's `update_rows` replaces the fork's `update_row`. Adaptation A. |
| 061ce1f* | Picked again, clean. |
| b33f1c6 | As is, clean. |
| 8f4d140 | As is, but adaptation B's line in `do_multishot_analysis`. |
| 4394eb3 | As is, but adaptation C. |
| b35294c* | Picked again, clean. |
| 4d30a1e | As is. Its twelve conflicts are the style pass and the fork's forms of later upstream commits (1beb61c's icons, 3a0888c's header fonts, 2b5d616's row height, and `open_in_editor` where 74e6127 calls `programs.open_file`); upstream's side throughout, then 1beb61c*, 3a0888c* (its `filebox.py` part), 2b5d616* and 74e6127* picked again. |
| 10d9022 | As is, clean. |
| 4edeed4 | As is. One conflict, an f-string. |
| 4954e44 | As is. One conflict, an f-string. |
| 6003d7c*, 9c52248* | Picked again onto 4394eb3's `start_analysis` and `end_routine`, replacing the fork's ports; then adaptation B. |

### Adaptations

- **A, results for a removed shot.** Upstream's `update_rows` skips a reply's
  shot that has no row. The fork reports it, since a result saved with
  `save_to_h5=False` is then kept nowhere (Ian kept this, as candidate B of
  step 2.1). Three lines in `update_rows`.
- **B, `lyse.paths` after a failed multishot pass.** Upstream's 9c52248 sends
  `done` when the routines after a failed one ran, and the filebox then forgets
  the pass's shots. The fork sends `error`, with 100 rather than None as its
  status, so that `do_multishot_analysis` puts the shots back for the next pass,
  and the pause state is still decided in the routine box.
- **C, `lyse.paths` itself.** `RoutineBox.do_analysis` and
  `AnalysisRoutine.do_analysis` take `(filepath, paths)`, a multishot box's
  shots since its last pass; upstream's take `filepath` only.
- The tests that stub `update_row` and call `todo()` move to `update_rows` and
  `start_analysis`/`end_routine`.

### Would upstream's `update_row` remove the differences?

Mostly, yes. Taking upstream's code removes everything the fork's `update_row`
held except A: its other changes were ports of upstream commits (0b010a9,
00a26a1, 3723c9b's signals) that `update_rows` already holds, or that are picked
again (061ce1f, b35294c). A can be offered upstream: report a reply's results
for a shot that has no row. With A upstream, 79f9db9 goes in with no adaptation.
B and C stay: they come from `lyse.paths`, a fork design, not from `update_row`.

## Size

About +400 −350 lines in `filebox.py`, `routines.py` and `widgets.py`, nearly
all upstream's, and A, B and C at about 20 lines together.
