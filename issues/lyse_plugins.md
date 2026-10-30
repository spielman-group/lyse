# Remote control, plugins and robust analysis

Ian's request: lyse needs a remote interface like runmanager's, so that
labscript-agents can drive it; plugins like BLACS's, called when a shot arrives
and when its analysis ends; analysis that a failing routine does not stop; and,
as the first plugin, one that deletes analysed default and repeated shots.
labscript-agents and labscript-optimization become such plugins.

## Requirements

### Remote interface

1. A client on another process or computer can, for each routine box: list its
   routines in order with whether each is active and how its last run ended;
   add classic scripts and GUI routine folders; remove, activate, deactivate,
   reorder and restart routines.
2. A client can control the rest of the interface: pause and resume analysis,
   Abort, Run the multishot routines, re-run given shots, re-run the failed
   shots, add and remove shots, read a shot's status, read the dataframe (as
   `lyse.data()` does now), save and load the dataframe, and set the
   stop-on-failure option (7): what the GUI controls, as runmanager's interface
   does, not the window's own presentation. lyse's configuration is not saved or
   loaded remotely.
3. Each remote command does what the matching button or menu item does, with
   the same result in the window, and refuses an unknown box, routine or shot
   by name.

### Plugins

4. A lyse plugin is written as a BLACS plugin is, in the suite's one plugin
   language, and is loaded or not as BLACS's are: a flag per plugin in the
   labconfig, in a `[lyse/plugins]` section.
5. lyse offers every loaded plugin a call at each of these points, and each
   plugin chooses what it does at each:
   - **shot received**: a shot has been added to the shots table, saying where
     it came from: runmanager, another client, the add-shots dialog or Load
     dataframe. It comes before any analysis of that shot, paused or not;
   - **single-shot done**: a shot's single-shot routines have ended;
   - **multishot done**: a multishot pass has ended;
   - **analysis done**: all analysis of a shot has ended: its single-shot
     routines and the multishot pass that includes it, whether each succeeded
     or failed. If no multishot routine is active, it follows single-shot done
     at once. A shot analysed again gets the calls again.

   Each call after analysis says how every routine involved ended.
6. A plugin can put its controls in named places of lyse's window, the menu bar
   and two areas (B), and can open windows of its own. A plugin's settings and
   window layout are saved and restored with lyse's configuration.

### Robust analysis

7. A failing routine never stops lyse, nor does a worker that dies: every call
   point of (5) still happens for that shot, and the next shot's analysis
   starts. An option, *stop a shot at its first failure*, chooses whether the
   shot's later single-shot routines run after one fails; it is off by default.
   A shot's analysis ends in a state of its own: finished, every routine done;
   failed, a routine did not end done but the rest ran; or stopped, its later
   routines skipped after a failure, by the option or by Abort. Analysis takes
   only shots not yet analysed, so it never takes a failed or stopped shot again
   by itself.
8. lyse records how each routine ended on each shot: done, error (an exception,
   `sys.exit` with a failure status, or its worker dying), aborted (by Abort),
   or not run, the words of the routine's status icon. The record is kept in
   the shots table, and so in the dataframe, and written into the shot file; a
   multishot routine's is on every shot of its pass. Analysing a shot again
   replaces its record.
9. The shots table shows each shot's state. A **Re-run failed** button, and its
   remote command, analyses all failed and stopped shots again, in order,
   without selecting them.

### The first plugin

10. After all analysis of a shot (5, analysis done), if the shot came from
    runmanager, every routine on it ended done, and it is a default shot or a
    repeat, the plugin deletes the shot file, under the suite's file lock. The
    shot's row stays in the table with its results, so a run of repeated shots
    still leaves its record, such as atom numbers. A default shot is recognised
    by the `sequence_index` of −1 that runmanager already writes in it, and a
    repeat by the `run repeat` of 1 or more that BLACS writes in it as it adds
    the `_rep` suffix. A shot added by hand is never deleted.
11. The plugin is on or off by its labconfig flag (4). BLACS's own
    `delete_repeated_shots` plugin is independent of it and can delete repeats
    before lyse analyses them, so a lab enables one or the other for repeats.
12. A row whose file is gone, deleted by the plugin or otherwise, is kept and
    shown as deleted in the status column. It is never analysed again: Re-run
    selected and Re-run failed skip it, unless its file is back, and no
    multishot pass offers it in `lyse.paths` again. `lyse.data()` and Save
    dataframe include it with its results; Load dataframe restores it as
    deleted; Remove takes it out of the table like any row. A file the plugin
    deleted brings no warning; one that vanished otherwise brings lyse's
    present warning.

### labscript-agents and labscript-optimization

13. Each becomes a lyse plugin, called at analysis done (5) for every shot.
    labscript-agents publishes its `shot_results` event for the shots it
    engaged; labscript-optimization reads the shot's cost and feeds its
    session. Each keeps its own window, with its own on/off control there.
    This is a full port: nothing of their GUI-routine forms remains, no
    `lyse.Routine` subclass and no `.lyse` folder in their examples.

## Architecture

**A. Remote interface** (1–3). `LyseServer` (`communication.py`) gains a
`handle_<command>` per command, and `LyseClient` (`client.py`) the matching
method, as runmanager's do (labscript-style rule 11). Beneath the GUI handlers,
which work on selections and dialogs, lie operations on explicit paths: the
routine box's add (`lyse.routines.add_routines`), remove, set active, reorder,
restart; the filebox's pause, Abort, Run, re-run, re-run failed and remove
shots; the app's save and load dataframe. The GUI handlers and the server's
handlers both call them, the server's on the GUI thread through `inmain`. They
raise `ValueError` naming an unknown box (`'singleshot'` or `'multishot'`) or
path, and return what a caller needs to check. Commands, in runmanager's forms:
`get_routines`, `add_routines`, `remove_routines`, `set_routines_active`,
`set_routine_order`, `restart_routines`; `get_analysis_paused`,
`set_analysis_paused`, `abort`, `run_multishot`, `rerun_shots`, `rerun_failed`,
`get_stop_on_failure`, `set_stop_on_failure`; `add_shot`, `remove_shots`,
`shot_status`, `get_dataframe`, `save_dataframe`, `load_dataframe`. `add_shot`
and `get_dataframe` exist; `add_shot` gains the shot's source, `'remote'` unless
the client names itself, as runmanager does.

**B. Plugin host** (4, 6, 11). `lyse/plugins/__init__.py` builds a
`labscript_utils.plugins.PluginManager` as `blacs/plugins/__init__.py` does:
plugin folders in `lyse/plugins/`, installed packages through the entry point
group `lyse.plugins`, flags in the labconfig's `[lyse/plugins]`. Two contexts
take contributions: `menus`, a `MenuContext` over lyse's menu bar, and
`areas`, where `{'context': 'areas', 'area': name, 'widget': widget}` adds the
widget to the end of a named area, `'analysis_controls'` beside Pause and Abort
or `'shots_toolbar'`. A plugin's window is its own widget, shown from a menu
entry it contributes, and its geometry is part of the plugin's save data.
`plugin_setup_complete` gets a dict: the app, the labconfig, the shots model
and the two routine boxes. At start-up lyse builds its boxes, whose threads
wait for shots, then instantiates the plugins, restores their data from the
configuration it autoloads, sets up the contexts and calls
`plugin_setup_complete`, and only then starts its server, so no call precedes
setup. Loading a configuration later restores plugin data again, as BLACS
does. At quit, after the save prompt, lyse fires no more calls, closes the
plugins, then ends the workers. Plugin data is saved in lyse's configuration.
Nothing new is needed in labscript-utils.

**C. Call points and a shot's attempt** (5, 7). An *attempt* of a shot begins
when the analysis loop takes it, a new shot or one a re-run marked not done. Its
single-shot phase runs the active single-shot routines, all of them, or up to
the first failure with the option on; the rest are not run. The phase ends the
shot in a state of its own, finished, failed or stopped (7), so analysis, which
takes only shots not yet analysed, moves to the next shot and never takes it
again unasked. `singleshot_done(path, outcomes)` then fires and the shot joins
the batch for the next multishot pass, failed or not; if no multishot routine is
active, `analysis_done(path, outcomes)` fires at once instead. A pass takes its
batch and the shots a failed pass offers again, less rows whose file is gone; at
its end `multishot_done(paths, outcomes)` fires, then `analysis_done` for each
shot of the batch whose attempt awaited this pass, once per attempt. Shots that
arrive during a pass join the queue for the next one. While paused, the shot
being analysed finishes its single-shot phase, no attempt starts and a due pass
waits, so its shots' `analysis_done` waits too. A shot removed during its
attempt gets no further call. `shot_received(path, source)`, with `source` one
of `'runmanager'`, `'remote'`, `'dialog'` and `'dataframe'`, fires on the GUI
thread inside the shots model's `add_files`, once the row is in, so before the
analysis loop, which asks the GUI thread for its next shot, can take it; the
incoming queue carries each path's source. The other calls fire on the analysis
thread. A slow handler starts its own thread, and touches the window through
`inmain`; a handler's exception is logged and analysis goes on (`fire_event`
does that).

**D. Outcomes and failures** (7–9, 12). `outcomes` maps each routine's name, as
its results group is named, to `'done'`, `'error'`, `'aborted'` or `'not run'`.
Workers reply done or error; a run the routine box sent Abort to ends as
`'aborted'`, whether its worker replies or the forced restart ends it. While it
waits for a reply it checks every second that the worker is alive, so a dead
worker's run ends as `'error'`; that closes the `KNOWN_ISSUES.md` hang. The
option replaces the filebox's automatic `pause_analysis()` on a single-shot
error, which goes; Pause stays a button, and Abort still pauses. The shots model
keeps each shot's state, not analysed, finished, failed, stopped or deleted,
beside its progress, draws it in the status column, and sets it from the shot's
present outcomes; the analysis loop's `get_first_incomplete` takes only shots
not analysed, and Re-run failed marks failed and stopped shots not analysed. At
the end of each phase lyse puts the outcomes in the shot's row, as `('lyse',
<routine>)` columns, and writes them into the shot file, in one open under the
h5 lock, as attributes of a `results/lyse` group, which the dataframe reads on
reload. A routine may write the same group, as routines can any group. A new
attempt's single-shot phase clears the group and the row's outcomes before it
writes its own, so a removed routine leaves no stale failure, and each pass of
an attempt replaces the multishot outcomes it wrote before. A failed write, of a
missing or read-only file, is reported in the output box; the row keeps the
outcomes. A row whose file is gone has the shots model's present deleted state,
which already keeps it out of analysis and Re-run selected; Re-run failed and a
failed pass's offer skip it too, and Load dataframe marks a row whose file is
missing as deleted.

**E. The first plugin** (10–11).
`lyse/plugins/delete_default_and_repeated_shots/`, after BLACS's
`delete_repeated_shots`. On `shot_received` it notes a path from runmanager and
forgets one from any other source, so a shot added again by hand is no longer
its. On `analysis_done`, for a noted shot whose outcomes are all `'done'` and
whose `sequence_index` is −1 or `run repeat` 1 or more, it queues the path for
its thread. That thread, under the shot's file lock, checks again that the row
is present, still noted and finished, and that the file's `results/lyse`
attributes are all `'done'`, so the outcomes were recorded; only then does it
delete the file, with labscript-utils' shared delete function, and mark the row
deleted, without the warning.

**F. labscript-agents and labscript-optimization** (13). Each package registers
a plugin under the `lyse.plugins` entry point and handles `analysis_done`.
They run in lyse's process, as BLACS's plugins run in BLACS's, so their slow
work stays on their own threads, as now. They read the dataframe through the
same selection `lyse.data()` uses, and labscript-optimization adds its columns
through the shots model's `update_rows`. Their `lyse.Routine` subclasses and
`.lyse` example folders go. The packages' owners port them (below).

| Requirement | Part |
| --- | --- |
| 1–3 | A |
| 4, 6 | B |
| 5 | C |
| 7–9, 12 | C, D |
| 10–11 | E |
| 13 | F, in labscript-agents and labscript-optimization |

## Needed elsewhere

- **runmanager:** its analysis submission names itself, `add_shot(path,
  source='runmanager')`.
- **labscript-utils:** a function that deletes a shot file under its file
  lock, which E and BLACS's `delete_repeated_shots` both call; and the
  `[lyse/plugins]` section documented beside `[blacs/plugins]`
  (`docs/source/labconfig.rst`, `example.toml`).
- **BLACS:** its `delete_repeated_shots` stays, calling the shared function.
- **labscript-agents**, once A–C exist: its plugin (F); ToolSpec bindings for
  the new lyse commands; its docs' line that it has no control of lyse goes;
  the clean-up that ran when its worker exited runs in the plugin's `close()`.
- **labscript-optimization**, once A–C exist: its plugin (F); its configuration
  found relative to the plugin's folder, where it was relative to
  `Routine.folder`; the hand-over to its session made from `analysis_done`
  without blocking, in place of the 2 s wait, with a late reply delivered when
  it comes; the clean-up that ran when its worker exited runs in `close()`.
