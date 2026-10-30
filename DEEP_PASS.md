# lyse deep pass

## Upstream stage

Upstream's `master..upstream/master`, to `4c6ae2b`: 109 commits, 68 of them not
merges. Ported by hand onto `CodeHealth`, each commit naming its upstream source.
Range: `8c04e38..5d6a338`.

| Upstream | What | Commit |
| --- | --- | --- |
| 9766125 | `sys.exit()` in a routine replies | d566e36 |
| fb3195a | A routine's traceback starts at the routine | 494b23a |
| 6c8312d | No kill lock across a whole run | f8af88c |
| 00a26a1, b35294c | A result of another dtype no longer stops analysis under pandas 3 | 644ced9 |
| 061ce1f | New columns made with `.loc` | 8d4f46f |
| 9b678b9 | Integer-indexing headers from a float column | 6d60c0c |
| 1dc53ea | An unreadable shot is skipped, not its batch | 0a2fceb |
| fed218a, 87703f8 | Shots stamped in a daylight saving transition hour load | 8d24890 |
| af6977d | A shot not yet run is not loaded | bfa817d |
| 8a6a5d5 | `lyse.spinning_top` | 0c90b60 |
| 74f1968 | The server starts once the filebox exists | d0b8212 |
| d68e43b | A worker starts without `autoload_config_file` | e7ace3b |
| 740dd2b | The Qt6 progress bar fills horizontally | 8a44abf |
| ae38219 | Units and expansion subgroups matched by name | bcb0b62 |
| ce9d019 | Load dataframe adds no index column | f32698e |
| 9ff5102 | `lyse.data()` and Save dataframe infer column types | 7d00a47 |
| 12cbb2c | Python floor, set to the suite's 3.13 | 51bd3c2 |
| 1beb61c, 9c71b2e | High-DPI icons; qtutils 4.4.1 | 4c7346b |
| 8af660f | `Run.get_globals` returns Python values, through labscript-utils' `get_shot_globals` since 40370bd; in `NEWS.md` | 541a929 |
| 455f19b, 197dbad | Workers start together, across both boxes | c88c079 |
| 8d7a297 | The output box's thread stops before lyse exits | 6a5d81f |
| 07866b2 | zprocess floor, 3.0.1, for the concurrent starts | 8d9ef86 |
| 23ff55a | A second lyse exits with a message when its port is in use, from labscript-utils' `ZMQServer` | 2325a16 |
| b8346c9 | The PyQt exit crash workaround is labscript-utils' `exec_qapplication`; the output box stops before it | 72c5efb |
| 6066df4, 001df5f, baffcac, b8a55d0, ff31a48, 0b010a9 | The shot's `data` groups in the dataframe; collisions reported; live results match a reload | 42dd396 |
| de837f5 | `lyse.data(columns=)` | 103d721 |
| 40370bd | `get_globals` and `globals_diff` take no `group` | 09e3762 |
| 28675dc | A classic routine compiled only when its source changes | 47cdb4f |
| d2bc831 | The column editor keeps its header items | 436c68e |
| 03e3c95, f347bca | Dialog shots queued in path order, digits by value | 3d866a1 |
| 9c573dd, 0c73f77 | The release workflow | 0955a92 |
| 25cd0bf | The screenshot | 6f5d316 |
| 6003d7c | A multishot routine's error does not pause analysis | 086ee76 |
| b956ecf | The excepthook installed explicitly | 18acd28 |
| 3a0888c, 2b5d616 | The suite's font and style; 20 px rows | bf26f5e |
| f424513 | Abort | 2b790e6 |
| 9c52248 | The routine box says whether a failed pass stopped, in the fork's form | e0e13dd |
| 4e593d4 | Logging arguments passed lazily | 5d6a338 |

`NEWS.md`'s Unreleased section, and the docs where they describe them, say what
changes for users and scripts.

### Not ported

- 0625332, a command-line shot path only if it is an HDF5 file: lyse no longer
  takes a shot path from the command line, by Ian's decision in step 2.4. That
  removal is offered upstream on `UpstreamBugFixes` (a643d08, from
  `upstream/master`).
- 9c52248 in upstream's form: its `done` after a failure would drop the failed
  pass's shots from the next `lyse.paths`; ported in the fork's form instead.

### Already in `CodeHealth`

50335c6, 2bf63cb, 3723c9b, feb7540, f9e6e72, and 74e6127 through labscript-utils'
`open_in_editor`, which now calls `programs.open_file`.

## Shots table stage

Upstream's large-table set, as `issues/shots_table_speed.md` designs it:
upstream's commits picked with `-x` in upstream's merge order, each fork change
its own commit. Range: `6d2ffd5..88c26db`; the commit after it records the
stage. Suite: 51 passed. `tools/table_benchmark.py`: every case within its limits at 4,000 and
50,000 shots; the longest stall, 0.16 s, removing half of 50,000.

| Commit | Upstream or fork | What |
| --- | --- | --- |
| dfe629e | ed71cc7 | Views uneditable by edit triggers. The fork's header set-up (d2bc831) kept beside it. |
| eccaadf | 0bb3acd | Removal in contiguous blocks. |
| a36053a | Fork, D | The removal prompt is the app's dialog. |
| bf57716 | 79f9db9 | `update_rows`: a routine's results in one call. The multishot loop keeps `status_percent`, which `lyse.paths` reads. |
| f04faf7 | Fork, A | Results for a removed shot are reported. |
| ef716d9 | 061ce1f | New columns made with `.loc`. |
| e4fcf47 | b33f1c6 | A one-row update set with `.at`. |
| 5a1d71f | 8f4d140 | No relayout after each routine. One conflict, the multishot line, as in 79f9db9. |
| b31d6d9 | 4394eb3 | `start_analysis` and `end_routine`: fewer calls into the main thread. Lazy logging (4e593d4) kept. |
| 05106d9 | Fork, C | `lyse.paths` through the routine box. |
| 8a0fa0e | b35294c | An integer column made float for a float result. |
| b36aedc | 4d30a1e | `ShotsTableModel` reads the table from the dataframe. Eleven of its twelve conflicts to upstream's side; the twelfth keeps `ROW_HEIGHT`, which a merged line uses, and `import subprocess` is back for upstream's viewer code until 74e6127. |
| 6ec8738 | Fork, F | The dataframe and the lists change inside one notification: a row removal for one block, else a model reset. |
| 3aa0578 | 10d9022 | The first shot not analysed is tracked. One conflict, F's lines beside it. |
| 1e2550e | 4edeed4 | The adding progress bar shows after 0.5 s. |
| 92988fc | 4954e44 | Duplicates checked with a set. |
| 9bcc8f9 | 1beb61c | `Icon`. The fork's own icon lines kept; duplicate imports dropped. |
| 3a6eccf | 6003d7c | A multishot error does not pause analysis. The fork's port goes; the docs keep the fork's introduction. |
| eb19b65 | 9c52248 | The routine box decides whether a failed pass stopped. One conflict, C's signature. |
| ee0b242 | Fork, B | A failed pass sends `error` with 100, so its shots are offered again. |
| 4d50cc6 | 3a0888c | The suite's font. One `setup_application` call, in upstream's place. |
| a26d0c7 | 2b5d616 | Fixed 20 px rows; 4d30a1e's resize to the first row goes. |
| 896179a | 74e6127 | `programs.open_file`. The fork's `open_in_editor` imports go. |
| eaa6b90 | Fork, D | File-opening errors are the app's dialog. |
| 735d557 | Fork, E | A GUI routine opens as its `lyse_routine.py`. |
| addefc6 | Fork | Tests 1 to 6 of the design, on the real model. |
| 88c26db | Fork | The benchmark. |

### Review

Codex reviewed `6d2ffd5..71972c6`. Its five findings are fixed, one commit each,
in `71972c6..0469ac3`. Suite: 51 passed.

| Commit | What |
| --- | --- |
| 3c79faa | Load dataframe beside loaded shots: the loader leaves gaps in the frame's index, and `add_files` now drops a duplicate by label, not by position. The same code is in upstream, so the fix is offered there. |
| cba7f60 | A benchmark case that raises ends its measurement, and the exception is raised again. |
| 1676414 | The removal test moves analysis past the rows removed first, so it covers resetting the first-incomplete cursor. |
| 33c10aa | The `lyse.paths` tests run two multishot routines; the second runs after the first fails. |
| 0469ac3 | The incoming loop's comment: runmanager submits a shot once BLACS has run it. |

Codex's re-check of `6d2ffd5..3f9699a` found one bug older than the stage, fixed
after the second upstream stage in `c5238d3`: the next routine could start, and
read the table through `lyse.data()`, before the previous routine's results
reached it. The routine box now applies each routine's results, waiting for the
GUI thread, before it ends the routine. Upstream has the same ordering.

## Second upstream stage

Upstream's `4c6ae2b..55c3154`: 19 commits, none of them merges, picked with `-x`
in upstream's merge order onto `3f9699a`, each fork change its own commit.
`master` is fast-forwarded to `55c3154`. Range: `3f9699a..e546531`; the commit
after it records the stage. Suite: 52 passed.

| Commit | Upstream or fork | What |
| --- | --- | --- |
| None | 4a00ada | Already in: the qtutils 4.4.1 and zprocess 3.0.1 floors (4c7346b, 8d9ef86). |
| 9e9e154 | a90eb3d | The new screenshot and introduction. The introduction is upstream's; the fork's facts come back in 3ceb216. |
| c760915 | 83ad998 | The API reference is the routine API; `conf.py` cleaned up. Conflicts to upstream's side. |
| 5ee886a | Fork | The Qt inventory is PyQt6's, the suite's binding, as before. |
| 6ec4e60 | 157899b | The examples and the README corrected; the 2014 LaTeX manual, which the fork had updated (e4296a8), goes. |
| e6b7753 | afeceee | Narrow routine box columns. The fork has no sort role (03c8825), so its `setSortRole` stays out. |
| None | c7f5146 | Already in, in the fork's form: the subprocess's `configure_qapplication` calls `setup_application`. |
| 10db051 | dfda62c | The import-order comment and the matplotlib 3.5 floor. The fork already used qtagg, set for classic workers only. |
| 14472e1 | 73548b5 | Screenshots retaken. |
| 1a5761b | 15cfe9a | Tooltips. |
| bf3a755 | 71dc091 | The licence's placeholders. |
| c550f56 | 9854a61 | `Plot`, `plots`, `delay_event` and `figure_to_clipboard` in the lyse namespace; the fork's `paths` and lazy `Routine` beside them. |
| None | 7fca1f2 | Already in, in the fork's form: labscript-utils' `exec_qapplication` runs the 500 ms timer and the Ctrl-C handler. |
| 93b67fb | d93be1b | Auto-raise buttons in place of the style sheets. The add button keeps the fork's menu. |
| 3866fa8 | ef12bfd | The Pause toggle; shorter labels. |
| 1cb675f | b5bd394 | Run and Re-run selected beside what they act on. |
| 80ca0fd | b34fcb4 | Run starts a multishot pass while paused. Its condition is combined with the fork's `multishot_failed`, so a pass that has just failed does not run again at once. A pass due on its own waits while paused, which is 9cff0f3's behaviour. |
| c094edb | f1502d2 | Columns to the right of the Shots toolbar. |
| d21a34a | afc0f72 | The qtutils 4.4.3 floor. |
| 3b385d1 | aef1528 | Screenshots retaken; the introduction names the controls. |
| 3ceb216 | Fork | The fork's facts in upstream's new text: runmanager sends the shots, `lyse.paths` and a failed pass's shots offered again, GUI routines and their output. Other docs name the new labels. |
| a872d44 | Fork | Test: while paused, only Run starts a multishot pass. |
| e546531 | Fork | `NEWS.md`. |

Outside lyse: labscript-utils' upstream 8f07c1a starts the 500 ms timer in
`setup_application`, before the port check, so that SIGTERM ends a second copy
showing the port-in-use dialog; the fork's starts in `exec_qapplication`, after
it. The environment has qtutils 4.4.1, below the 4.4.3 floor; the suite passes
with it, but 4.4.3 draws the output box dark under a style sheet and its scroll
bar grooves not black.

## Against upstream

`git diff 55c3154` (upstream's `master`) of the files the shots table and second
upstream stages touched, in lines changed: `filebox.py` 214, `routines.py` 534,
`widgets.py` 107, `__main__.py` 425, `analysis_subprocess.py` 634, `__init__.py`
517. `filebox.ui`, `main.ui` and `plot_window.ui` are upstream's;
`routinebox.ui` differs by the add button's menu. `ShotsTableModel` is
upstream's. In `DataFrameModel` and the routine box's analysis, what differs is
A to F, `lyse.paths`, the review's loading fix, `multishot_failed` and the
results applied by the routine box, with `super()` and a few docstrings and
comments of the style pass. The rest predates these stages: the style pass,
labscript-utils' `TableView`, file dialogs and `exec_qapplication`, GUI
routines, Abort, and two earlier ports in the fork's form, the incoming loop's
warning as a lambda where upstream has `_shot_warning`, and its log line without
upstream's `isEnabledFor` guard.

## Compatibility audit

Every compatibility path the campaign or the upstream stage added, against
`master` (upstream). None serves only the fork's other tiers or nothing, so none
was removed. Two keep users' files working:

- The INI-to-TOML conversion: a plot window's geometry saved by upstream lyse in
  `lyse-<name>.ini` (`analysis_subprocess.py`, `PlotWindow._migrate_legacy_geometry`
  and the `QByteArray` branch of `_decode_geometry`), and lyse's own configuration
  file, which `load_configuration` reads through labscript-utils'
  `load_appconfig`. Each converts the file to TOML at once and warns. Ian kept the
  conversion: "INI files are converted to TOML at once, with no user
  intervention, and many labs still have them" (labscript-utils `DEEP_PASS.md`,
  step 1 entries 1, 3 and 8, at 9f40c73).
- A shot file without a `shot_id` attribute loads with `shot_id` `''`
  (`dataframe_utilities.get_nested_dict_from_shot`): every shot made before the
  fork's runmanager wrote it, or not queued by runmanager. Kept on Ian's word
  (C-LY1).

