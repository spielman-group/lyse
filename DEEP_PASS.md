# lyse deep pass

A planning document for the code-health campaign (`issues/code_health_campaign.md`
in the meta repo). It lists candidates for Ian's review and is deleted when the
pass is done. Nothing is changed until Ian has reviewed each entry.

## Step 1: contract

What each existing test protects. Marks: **keep**, **rename** (the name does
not state the behaviour), **fix** (the behaviour is real but the test carries a
stale or dead part), **cut** (protects nothing worth keeping).

All 44 tests were written by the fork; upstream had none. Most were written
with a mutation that made each fail, slice by slice. The two least direct, the
status column's paint and the removed shot's results, were broken again on a
scratch copy for this list.

### Totals

- Test files: 8, plus `conftest.py`. Tests: 44, with 17 subtests.
- Marks: keep 42, rename 1, fix 1, cut 0. Approved by Ian; the rename and the fix are made.

### tests/test_analysis_subprocess.py

The analysis worker, run as a real process as lyse runs it.
- `test_a_theme_change_repaints_the_window_without_error`: a plot window follows
  a light/dark switch, repainting its children. **keep**.
- `test_a_figure_named_by_a_string_gets_a_window_that_keeps_its_geometry`:
  `plt.figure('name')` gets a plot window, and its size is restored. **keep**.
- `test_shots_are_analysed_in_turn_with_the_controls_saved_first`: a GUI
  routine runs each shot with the controls' values as saved before it. **keep**.
- `test_a_failed_run_replies_error_without_earlier_results`: a run that raises
  replies error, and the worker goes on. **keep**.
- `test_quit_waits_for_run_and_calls_close_once`: quitting waits for a running
  `run()`, then calls `close()` once on the GUI thread. **keep**.
- `test_a_routine_that_cannot_load_fails_every_analysis_until_restart`: a
  routine that does not load fails each analysis until restarted. **keep**.
- `test_show_gets_no_reply_so_the_next_analysis_gets_its_own`: "Show windows"
  sends no reply that the next analysis could take as its own. **keep**.
- `test_a_routine_icon_is_its_window_and_process_icon`: a routine's `icon` is
  its window's and its process's icon, lyse's by default. **keep**.
- `test_an_inactive_routine_starts_with_its_window_hidden`: only an active
  routine's window shows at startup. **keep**.
- `test_only_lyse_ends_a_worker`: a request to quit, as Cmd-Q makes, closes a
  worker's windows and leaves it running, for both kinds of worker. **keep**.

### tests/test_communication.py

`lyse.data` and `LyseClient` against a real `LyseServer`.
- `test_the_most_recently_engaged_sequences_are_returned`: `n_sequences=N`
  returns the N sequences engaged last, whatever the row order. **keep**.
- `test_rows_are_chosen_by_a_list_of_filepaths`: `where={'filepath': [...]}`
  with a list, array or set. **keep**.
- `test_rows_are_chosen_by_equality_on_a_nested_column`: `where` on a column
  such as `('fit', 'atoms')`. **keep**.
- `test_a_column_not_in_the_dataframe_is_refused_by_name`: **keep**.
- `test_rows_are_chosen_after_n_sequences`: `where` filters within the
  `n_sequences` chosen. **keep**.
- `test_a_submitted_shot_reaches_the_incoming_queue_as_a_local_path`: **keep**.

### tests/test_filebox.py

- `test_results_for_a_shot_with_no_row_are_reported_and_the_list_stays_live`:
  results for a shot removed while its routine ran are reported, and the shot
  list keeps updating. **keep**. Fails when the report is removed, and when the
  model's signals are blocked before the missing row returns. The second half
  is a real bug fix; the report is candidate B of step 2.1.

### tests/test_main_window.py

- `test_a_theme_change_repaints_the_window_without_error`: the main window
  follows a light/dark switch. **keep**.
- `test_a_dataframe_with_object_columns_is_saved_beside_its_shots`: saving a
  real dataframe, whose columns include bytes and arrays. **keep**.
- `test_labscripts_engaged_in_the_same_second_are_saved_apart`: two sequences
  engaged in one second do not overwrite each other's file. **keep**.

### tests/test_multishot_paths.py

`lyse.paths`, through the routine box's real analysis loop.
- `test_a_multishot_routine_sees_the_shots_analysed_since_its_last_pass`:
  **keep**.
- `test_after_a_failed_pass_its_shots_are_offered_again`: **keep**.
- `test_a_singleshot_routine_sees_none`: **keep**.
- `test_a_failed_pass_is_not_run_again_until_analysis_resumes`: **keep**.

### tests/test_routine.py

`lyse.Routine` and its window, built as a worker builds them.
- `test_the_one_routine_class_is_found_whatever_its_names`: **keep**.
- `test_no_class_or_two_classes_is_an_error`: **keep**.
- `test_a_saved_widget_takes_its_saved_value_and_reaches_the_snapshot`:
  **keep**.
- `test_bad_names_duplicate_names_and_unsupported_values_are_errors`: **keep**.
- `test_a_named_figure_is_made_once_and_kept_when_hidden`: **keep**.
- `test_ctrl_c_copies_the_figure_only_from_within_its_dock`: **keep**.
- `test_load_ui_finds_a_relative_file_in_the_routine_folder`: **keep**.
- `test_the_layout_comes_back_with_the_output_dock_shown`: **keep**.
- `test_get_sequence_and_data_default_to_the_analysis_shots`: **keep**.
- `test_results_go_in_the_class_name_unless_the_routine_sets_a_group`:
  **keep**.
- `test_settings_are_kept_per_full_path_and_bad_entries_are_dropped`: **keep**.
- `test_an_unreadable_file_is_set_aside_and_saving_resumes`: **keep**.
- `test_output_reaches_the_output_box_and_not_lyse`: **keep**.

### tests/test_run.py

`lyse.Run` writing results to the shot file and the shot's row.
- `test_a_run_saving_only_to_the_dataframe_never_opens_the_file`: **keep**.
- `test_a_first_write_creates_the_group_and_writes_in_one_open`: **keep**.
- `test_a_result_not_yet_saved_is_reported_as_before`: reading a result from a
  group that was never written raises that the group does not exist. **rename**
  to `test_reading_a_result_never_saved_raises_that_its_group_does_not_exist`:
  "as before" narrates history.
- `test_the_results_groups_are_empty_before_the_first_write`: `get_attrs` on
  `results` and on the routine's group gives `{}` before any write, as
  upstream's did; any other missing group raises. **fix**: its docstring, "As
  they were when construction created them", narrates history.
- `test_a_result_outside_the_results_group_cannot_skip_the_file`: candidate A
  of step 2.1. **keep** while A stands.
- `test_a_shot_file_that_is_not_there_is_refused_at_construction`: **keep**.

### tests/test_widgets.py

- `test_a_shot_being_analysed_paints_without_error`: the status column paints
  a shot's progress bar under Qt6. **keep**. Fails when the bar takes its font
  metrics from `QApplication.fontMetrics()`, which Qt6 removed.

## Step 2.1: behaviour no one asked for

The source is `git diff master CodeHealth`: 112 commits, lyse's core
+1678 −834 lines. Almost every behaviour the fork added traces to a request or
a suite-wide decision.

| Behaviour | Commits | Where it was asked for |
| --- | --- | --- |
| TOML config files; legacy INI read and converted | 82b58f2, e7bb157, bc02be8 | `toml` and `RunmanagerTOML` in `CODEX.md`. The INI path is for step 2.4 |
| Config path in the window title | c4182ca | `UpdatedTitleBars` in `CODEX.md` |
| macOS icons and display name | 6906087, 611ceef, 48f0a2d | `MacOS` in `CODEX.md` |
| PyQt6, and Qt6's removed APIs | b872641, 2fd7d37, 2bd3060 | The suite's move to PyQt6; `Qt6Fixes` in `CODEX.md` |
| Theme changes reach the plot and main windows | 7e826c6, b556337, 317decb, 6a9c60f | Fixes to upstream's theme handling, which listened for events Qt does not send |
| Current pandas | 8a70a37, 747cc21, 738df15, 67db467 | pandas removed the APIs lyse called |
| `shot_id` column, `n_sequences` by engage time, `lyse.paths`, `save_to_h5=False`, `where=`, result groups made on first write | 9a4ad0f, 084acf7, 88dd2ac, 4e5b71c, 99d3adb, 09acd22, 6286645, 0964797, 41c22cf, 959c1ed | `RunmanagerControl` in `CODEX.md`, lyse's part |
| A figure named by a string | 0ce84f1, 7c11b4f | Bug: `plt.figure('name')` raised, and the routine's plots never showed |
| Sequences engaged in the same second saved apart | bc3dd41 | Bug: one overwrote the other's dataframe file |
| `LyseServer` and `LyseClient` | acb7992 | `issues/server_interfaces_2026_09_25.md` |
| Restart, remove and quit never freeze lyse or start two workers; a non-zero exit is reported; a configuration load restarts the routines it shares; one `UiLoader` per worker | 8d02bfd, 91de899, 4f90e31, 4b06ce6, baa5775, edd1a82, 1f90378 | Ian's 2026-10-01 lyse bug list, worked on a `BugFixes` branch. Several are guards for step 2.3 |
| GUI routines, with their window, docks, settings, output, icon, startup visibility and Dock tile; only lyse ends a worker; the add menu; examples and docs | 8296f6d..f644cc7, 2ad56fb, 3a1260f, a016959, e4296a8, a11296a, 23b7b98, bb4f296, 9e089ca, 6f14dd8, 4ee46dd | `issues/class_routines_2026_09_30.md`, agreed with Ian, and his bug reports during it |
| The plot-class error names the real base class | f396a83 | Ian |
| A worker ends with lyse, through `exec_qapplication` | 83cfd6c | Ian |
| Shared code, version scheme, docs and the earlier campaign rounds | bc89825, ce2b7f9, 0bc790d, 7e96ae4, 010cd03, 2aa9328, f4c8e8e, 3a0dff4, b0a872e, 8f4eb9a, aafd7e5, 03c8825 | `EditFile`, `ReviewFixes` and the campaign's approved rounds |
| Test environment and test rules | 3d32196, 195d8c8, 376a4b9, a17040e, 6b95381, 3b3bdcb, 733eeec, b75bf67 | The workspace's test rules and Ian's rule 13 |

`Run.get_attrs` giving `{}` for the results groups before the first write keeps
upstream's behaviour, since upstream made the groups at construction. It is
protected, not a candidate.

### Candidates

Ian kept both.

Both came from a code review of the `RunmanagerControl` work, not from a
request. Each prevents a result being lost without a word.

| | Behaviour | Cost | Recommendation |
| --- | --- | --- | --- |
| A | `Run.save_result(..., save_to_h5=False)` with a group outside `results` raises `ValueError`, since such a result would be saved nowhere (959c1ed) | 3 lines and one test | keep |
| B | Results for a shot removed while its routine ran are reported in lyse's output as not recorded (8dd7674). The same commit's unblocking of the model's signals on that path is a bug fix, not a candidate | 3 lines, tested with the bug fix | keep |

Searched: `CODEX.md`, the meta repo's `issues/` and their history, shared
memory, and each commit's message and branch. Users reached: the operator
through lyse's window, analysis routines through `lyse.Run`, `lyse.data`,
`lyse.paths` and `lyse.Routine`, labscript-optimization as a GUI routine, and
runmanager through `LyseClient`.

## Step 2.2: duplication

Searched: the librarian's map (`labscript-library/capabilities.md`,
`inventory/lyse.txt`, `notes/lyse.txt`), then `git grep` in every repo for each
lyse helper whose job could be done elsewhere. labscript-utils' step 2 rounds
already moved lyse's version, dialogs, file dialogs, left-click views, theme and
first-paint mixins, event loop, `ensure_str`, editor launch, quit prompt and row
moving into shared code.

### Candidates

| | Solved more than once | Should it exist | Its one copy |
| --- | --- | --- | --- |
| C | The bookkeeping after a configuration load: record the file and the loaded state, name the file in the Save item, enable Save as and Revert, and set the title. lyse `__main__.py:267` `load_configuration`, runmanager `__main__.py:4125`, runviewer `__main__.py:389`, about 6 lines each | Yes: it is what makes Save, Revert and the quit prompt work after a load | A `LabscriptGuiApplication` method in labscript-utils, called at the end of each app's `load_configuration`. Not visible to a user. Touches labscript-utils, lyse, runmanager and runviewer |
| D | The main window's size, position and splitters, saved with the configuration and restored on load. lyse `get_save_data` and `load_window_geometry_configuration` (`__main__.py:250`, `295`, with `utils/gui.py:get_screen_geometry`), and runviewer `get_save_data` and `load_configuration` (`__main__.py:378`, `398`). runmanager saves none | Yes: upstream lyse saved it, with the same-screens check | `LabscriptGuiApplication`, reading the splitters from the `geometry_keys` each app already lists, keeping lyse's keys so saved files still restore. runviewer would gain lyse's rule, restoring nothing after a change of screens: visible to a runviewer user. Touches labscript-utils, lyse and runviewer |

### Not candidates

- `Run.get_globals` (`__init__.py:991`) and labscript-utils'
  `shot_utils.get_shot_globals`: `Run.get_globals` is upstream's API, takes a
  group and returns the attributes as stored; `get_shot_globals` converts their
  types. Different jobs.
- The plot window's geometry (`analysis_subprocess.py:140`), a GUI routine's
  layout (`routine.py:286`) and the window state carried across a change of
  Plot class (`analysis_subprocess.py:270`): three jobs, each on Qt's
  `saveGeometry`. The plot window's legacy-settings migration is for step 2.4.
- `get_screen_geometry`, `scientific_notation`, `flatten_dict`,
  `figure_to_clipboard` and `tempfile2clipboard`: no copy in another repo.
- `AnalysisWorker` and `GuiWorker`: one `mainloop`; `GuiWorker` overrides
  `quit`, `analyse` and `windows` by design.
- runmanager's `ItemDelegate` and lyse's: the same name, different jobs.
  runmanager's sets row height and column width, draws grid lines and edits
  multi-line values; lyse's draws the status column's progress bar.

Ian approved C, and D across lyse, runmanager and runviewer, with runmanager
gaining window geometry. labscript-utils 6c9fdc3; lyse cea6a15; runmanager
6e80ffb; runviewer 3e2f02f. A real start of each restored the window. Quitting now
treats `screen_geometry` as geometry too, so a change of screens alone is saved
without asking.

## Step 2.3: root causes

Each guard the fork added, from `git diff master CodeHealth`, traced to its
cause.

### Candidates

| | Guard | Cause | Fix at the cause |
| --- | --- | --- | --- |
| E | `routines.py`: `send_analysis` and `end_child` check `worker.poll()` before each put, since a put to a worker that has exited waits forever (open item 2 of the meta repo's `lyse_issues_2026_10_01.md`) | zprocess: `WriteQueue.put()` without a timeout waits until the queue can send, which it never can once the process at the other end has gone | zprocess's owner makes a put to an exited process raise, then lyse drops its `poll()` checks. The shutdown check beside one of them stays: it keeps a shot from a worker that is quitting. Not visible to a user |
| F | `routine.py:63`: `RoutineWindow.closeEvent` refuses the close and hides the window, so that Qt does not quit the worker on its last window closing | The worker quitting on any request to quit, which `QuitFilter` now refuses at its cause (6f14dd8) | Delete the four lines: Qt's own close hides the window. `test_a_named_figure_is_made_once_and_kept_when_hidden` loses its `assertFalse(self.window.close())`, which pins the refusal, and keeps checking that the closed window is hidden with its figure kept. Not visible to a user |

### Not candidates

- One `UiLoader` per worker (`routine.py:41`), because qtutils' loader erases
  the widgets registered on earlier loaders (open item 3). The cause is in
  qtutils, outside the suite, and one shared loader is the simpler design
  anyway.
- The Dock tile's icon sent again on each switch to Regular
  (`analysis_subprocess.py`, `DockIcon`): the Dock drops an icon sent to a
  process without a tile, a macOS cause.
- The shutdown check and the send made in one step on the GUI thread
  (`routines.py`, edd1a82), and a restart only after the old worker has exited
  (91de899): lyse's own ordering, fixed where the race was.
- `pandas.to_numeric` in a `try` (`__main__.py`, saving the dataframe): the
  same behaviour as pandas' removed `errors='ignore'`.
- A deleted saved widget left out of the controls (`routine.py`,
  `read_saved_widgets`): designed behaviour, not a guard.

## Step 2.4: dead code and compatibility leftovers

Searched: every name lyse defines, counted with `git grep -w` across all repos,
hyde included; the librarian's mapping list (`issues/seen_while_mapping.md`,
lyse's section); and each branch's commits against `Development`. The
INI-to-TOML conversion stays, by Ian's decision in labscript-utils' pass, so
lyse's INI paths and the plot window's settings migration are not candidates.

### Candidates

| | What | Evidence | Removal visible to |
| --- | --- | --- | --- |
| G | `AnalysisWorker.reset_figs` (`analysis_subprocess.py:572`), a stub whose body is `pass` | Referenced nowhere but its definition | no one |
| H | Loading a `.msg` dataframe: the `*.msg` filter and the `read_msgpack` branch of `on_load_dataframe_triggered` (`__main__.py:330-350`) | pandas removed `read_msgpack` in 1.0, so the branch only reports that it cannot load the file | an operator, who no longer sees `.msg` offered; none can be loaded today |
| I | Running a classic script outside lyse with a shot path: `if len(sys.argv) > 1` in `__init__.py:65` sets `lyse.path` | Upstream deprecated it with a FutureWarning asking users to speak up. It also fires on any import with an argument, under pytest included | anyone who runs `python script.py shot.h5` |
| J | The Qt5 names for matplotlib's Qt backend: `backend_qt5agg` (`analysis_subprocess.py:43`) and `MPLBACKEND = "qt5agg"` (`:737`) | The suite is PyQt6; matplotlib's `qtagg` serves any binding, and `routine.py` already uses `backend_qtagg` | no one |

Not candidates: `lyse.Run`'s methods that no repo calls (`get_wait`,
`get_units`, `trace_names`, `get_results`, `save_result_arrays` and others) are
the analysis API for users' scripts. `Sequence`'s methods that raise
`NotImplementedError` are upstream's. `LyseServer.handle_get_dataframe` is
reached by name. All 25 `on_` handlers are connected.

### Branches

Every branch other than the tiers and `CodeHealth`. lyse has no
`UpstreamBugFixes` and no open pull request.

| | Branch | Content | Proposal |
| --- | --- | --- | --- |
| K | `MacOS` (local and origin) | One merge not in `Development`, whose 7 lines (the worker's app name, icon property and Qt setup) `Development` already has | delete |
| L | origin `Collecting-for-fork` | 66 commits, 2023-2025: upstream merges and Ian's first prototype of per-worker windows, which ClassRoutines replaced | delete |
| M | origin `maintenance/3.0.x` | Upstream's 2020 maintenance branch, kept upstream | delete from the fork |
| N | `spielman_merged/SaveDataframeAndNamedFigures` | Every commit is in `Development` and `Production` | delete |
| O | `spielman_retired/ClassRoutinesThrowAway` | The first draft of GUI routines, which ClassRoutines replaced, with its spec | delete |
| P | `spielman_retired/share` (local and origin) | 8 commits moving lyse onto shared helpers, which the campaign's step 2 has done | delete |
