# Remote control, plugins and robust analysis

Ian's request: lyse needs a remote interface like runmanager's, so that
labscript-agents can drive it; plugins like BLACS's, called when a shot arrives
and when its analysis ends; analysis that a failing routine does not stop; and,
as the first plugin, one that deletes analysed default and repeated shots.
labscript-agents and labscript-optimization become such plugins.

The shared parent state machine and child process are implemented in
`labscript_utils.workers`, used by BLACS and the optimizer. The agent plugin
uses background threads in lyse's process. This document describes the current
software; hardware checks are outside the offline campaign.

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
   by name. Removing shots rejects the whole request if any member is currently
   running; the GUI reports the same refusal.

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
6. A plugin can put its controls in named places of lyse's window: the menu bar,
   two areas, or a finger tab beside Output (B). The tabs cannot be closed.
   lyse saves the selected tab by plugin identity, falling back to Output if the
   plugin is unavailable. Plugins save their operational settings; changing tabs
   never starts or stops a plugin.

### Robust analysis

7. A failing routine never stops lyse, nor does a worker that dies: every call
   point of (5) still happens for that shot, and the next shot's analysis
   starts. An option, *stop a shot at its first failure*, chooses whether the
   shot's later single-shot routines run after one fails; it is off by default.
   A shot's analysis ends in a state of its own: done, every routine done;
   failed, a routine did not end done but the rest ran; stopped, its later
   routines skipped after a failure by the option; or aborted, by Abort. While a
   multishot routine is active, a shot whose single-shot routines have ended
   awaits its pass before it takes one of these. Analysis takes only shots not
   done yet, so it never takes a failed, stopped or aborted shot again by
   itself.
8. lyse records how each routine ended on each shot: done, error (an exception,
   `sys.exit` with a failure status, or its worker dying), aborted (by Abort),
   or not run, as the routine's status icon then shows. The record is kept in
   the shots table and dataframe, including saved dataframes; a multishot
   routine's is on every shot of its pass. Analysing a shot again replaces its
   record. Execution outcomes are not written into shot files; user analysis
   results retain their existing storage behavior.
9. The shots table shows each shot's state. A **Re-run failed** button, and its
   remote command, analyses all failed, stopped and aborted shots again, and
   those waiting to retry a failed multishot pass, in order, without selecting
   them.

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
    session. Each contributes its panel as a tab, labelled Agent or Optimizer,
    rather than opening a floating window. Agent has an Enable agent checkbox;
    its saved setting is restored, initially off when there is no saved setting.
    The server stays running while disabled, but tool calls to control labscript
    perform no requested operation and report that agent control of labscript is
    currently offline. Status remains readable through `outstanding`.
    Already-submitted shots continue and their outcomes remain accounted for.
    This is a full port: nothing of their GUI-routine forms remains, no
    `lyse.Routine` subclass and no `.lyse` folder in their examples. Both panels
    run on lyse's GUI thread. The optimizer's session runs in a worker process
    (14); the agent's MCP server, tool execution and batch watcher use background
    threads in lyse's process.

### Plugin workers

14. A plugin can use the same shared parent and child implementation as a BLACS
    device tab to run work in separate processes. Its GUI-thread generator
    states yield jobs and resume with their results while Qt continues serving
    events. The parent creates and ends workers; a restart completes termination
    before creating replacements. At quit, lyse calls the plugin's `close()` on
    a background thread while its GUI thread keeps serving, so worker cleanup
    can wait without blocking Qt. The first plugin (10) needs no worker and has
    none.

## Architecture

**A. Remote interface** (1–3). `LyseServer` (`communication.py`) gains a
`handle_<command>` per command, and `LyseClient` (`client.py`) the matching
method, as runmanager's do (labscript-style rule 11). Beneath the GUI handlers,
which work on selections and dialogs, lie operations on explicit paths, split
from those handlers as runmanager's `engage` is from `on_engage_clicked`: the
routine box's `remove_routines`, `set_routines_active`, `reorder_routines`
(taken from `lyse.routines.add_routines`, which already reorders by path) and
`restart_routines`, beside the existing `add_routines`; the filebox's
`set_analysis_paused`, `abort`, `run_multishot`, `rerun_shots` (from
`mark_selection_not_done`), `rerun_failed` and `remove_shots` (from
`remove_selection`); the app's `save_dataframe` and `load_dataframe`, where an
empty dataframe raises in place of the dialog. The GUI handlers and the server's
handlers both call them, the server's on the GUI thread through `inmain`. They
raise `ValueError` naming an unknown box (`'singleshot'` or `'multishot'`) or
path, and return what a caller needs to check. Commands, in runmanager's forms:
`get_routines`, `add_routines`, `remove_routines`, `set_routines_active`,
`reorder_routines`, `restart_routines`; `get_analysis_paused`,
`set_analysis_paused`, `abort`, `run_multishot`, `rerun_shots`, `rerun_failed`,
`get_stop_on_failure`, `set_stop_on_failure`; `add_shot`, `remove_shots`,
`shot_status`, `get_dataframe`, `save_dataframe`, `load_dataframe`. `add_shot`
and `get_dataframe` exist; `add_shot` gains the shot's source, `'remote'` by
default, which runmanager's submission sets to `'runmanager'`. `shot_status` is
keyed by path, where runmanager's is keyed by shot ID.

**B. Plugin host** (4, 6, 11). During startup, lyse creates and owns a
`labscript_utils.plugins.PluginManager` using its existing labconfig:
plugin folders in `lyse/plugins/`, installed packages through the entry point
group `lyse.plugins`, flags in the labconfig's `[lyse/plugins]`. Importing
`lyse.plugins` supplies interfaces and UI contexts without discovering plugins.
Menu items come from `get_menu_contributions`, rendered by a `MenuContext` on which lyse
registers its menu bar as the `'menubar'` location, as BLACS does. Widgets come
from `get_ui_contributions` through a context lyse registers as `'areas'`, a
class with `add(plugin_name, contribution, data)` after BLACS's notifications:
`{'context': 'areas', 'area': name, 'widget': widget}` adds the widget to the
end of a named area, `'analysis_controls'` beside Pause and Abort or
`'shots_toolbar'`. The `'tabs'` context is `lyse.plugins.Tabs`, with the same
`add(plugin_name, contribution, data)` interface. A contribution
`{'context': 'tabs', 'widget': panel, 'label': 'Agent', 'icon': optional_qicon}`
embeds one QWidget panel per plugin; omit `icon` if none is needed. The fixed
Output page and the nonclosable left-side FingerTabWidget live in `main.ui`,
using `labscript_utils.qtwidgets.fingertab.FingerTabWidget`. The OutputBox and
its redirection port stay attached to that page. lyse records the selected
plugin's identity in `lyse_state.selected_tab`, with an empty string for Output;
it does not save the page's numeric position. A missing plugin selects Output.
Plugins do not save visibility or floating-window geometry for embedded pages,
hide or reparent them during restore, or add menu actions to show them. Their
labconfig loading flags remain separate from their operational settings. The
setup dict, keyed as BLACS's is, holds the labconfig, the `ui`, the shots model and the two
routine boxes. At start-up lyse builds its boxes, whose threads wait for shots,
then instantiates the plugins, restores their data from the configuration it
autoloads, sets up the contexts and calls `plugin_setup_complete`, and only then
starts its server, so no call precedes setup. Loading a configuration later
restores plugin data again, as BLACS does. Plugins start their background services
in `plugin_setup_complete`, before the server; the optimizer queues its worker's
initialisation there (G). At quit, after the save prompt, lyse
fires no more calls, starts no more analysis, and ends the routines' workers,
whose locks a pending deletion may wait on; then it calls the plugins' `close()`
from a thread of its own while its GUI thread keeps serving, so that a `close()`
may wait for its workers to end (G) and for its own threads, which may need the
GUI thread. Plugin data is saved in lyse's configuration.

**C. Call points and a shot's attempt** (5, 7). An *attempt* of a shot begins
when the analysis loop takes it, a new shot or one a re-run set not done. Its
single-shot phase runs the active single-shot routines, all of them, or up to
the first failure with the option on; the rest are not run.
`singleshot_done(path, outcomes)` then fires. If a multishot routine is active,
the shot then awaits multishot: the single-shot loop does not take it again, the
first plugin does not delete it, and the next pass takes it, failed or not. If
none is active, its attempt ends at once. A pass takes every shot awaiting
multishot; at its end `multishot_done(paths, outcomes)` fires, and each shot it
took ends its attempt. An attempt ends with `analysis_done(path, outcomes)` and
sets the shot's state from all its outcomes: done, failed, stopped or aborted
(7). A shot whose pass failed is left awaiting multishot instead, with that
pass's outcomes, so that the next pass retries it, as `lyse.paths` does now, in
a new attempt of its multishot phase. Abort leaves a pending pass's shots
awaiting it, and a pass it interrupts ends as a failed pass. A shot whose file
vanished while it awaited the pass ends its attempt there as deleted, and its
path is not offered to the routines. A re-run asked for a shot whose attempt is
under way takes effect when that attempt ends, which then sets the shot not done
in place of its state. Shots that arrive during a pass join the queue for the
next one. While paused, the shot being analysed finishes its single-shot phase,
no attempt starts and a due pass waits, so its shots' `analysis_done` waits too,
unless Run, the button or the remote command, starts the pass. A running shot,
or any member of an executing multishot batch, cannot be removed until all of
that phase's callbacks return. Selection and reservation happen on the GUI
thread; a shot removed before reservation is not started. Queued shots and
shots waiting for a pass or retry remain removable. A file that disappears
ends that shot as deleted, retaining collected outcomes and issuing terminal
notifications without scheduling further analysis for it. `shot_received(path, source)`, with
`source` one of `'runmanager'`, `'remote'`, `'dialog'` and `'dataframe'`, fires
on the GUI thread inside the shots model's `add_files`, once the row is in, so
before the analysis loop, which asks the GUI thread for its next shot, can take
it. Each path carries its source from where it was queued, the server or the
add-shots dialog, through the incoming loop's merging to `add_files`. The model
keeps receipt provenance alongside each accepted row and discards it when that
row is removed. A retained row keeps its source when reanalysed. Load
dataframe adds most of its rows itself, with `add_files(..., done=True)` on the
GUI thread, and queues the files changed since saving; both pass `'dataframe'`,
and the rows it adds keep their loaded results and take their state from their
loaded outcomes, done if they have none, or deleted where the file is missing; a
loaded shot never awaits multishot. The other calls fire on the analysis thread.
A slow handler starts its own thread, and touches the window through `inmain`; a
handler's exception is logged and analysis goes on (`fire_event` does that).

**D. Outcomes and failures** (7–9, 12). `outcomes` maps each routine's name, as
its results group is named, to `'done'`, `'error'`, `'aborted'` or `'not run'`.
Workers reply done or error; a run the routine box sent Abort to ends as
`'aborted'`, whether its worker replies or the forced restart ends it. While it
waits for a reply it checks every second that the worker is alive, so a dead
worker's run ends as `'error'`; that closes the `KNOWN_ISSUES.md` hang. The
option replaces the filebox's automatic `pause_analysis()` on a single-shot
error, which goes; Pause stays a button, and Abort still pauses. The shots model
keeps each shot's state: not done, awaiting multishot, done, failed, stopped,
aborted or deleted, the last renaming `deleted_off_disk` and
`ROLE_DELETED_OFF_DISK`. Awaiting multishot is the per-shot form of
`FileBox.analysed_since_multishot`, which goes: a pass's paths, its
`lyse.paths`, are the shots in that state, in the order they came to it, as now,
kept by a number each shot takes as it enters the state. It draws the state in
the status column, where `status_percent` shows only the progress of the
routines running. `get_first_incomplete` reads the state and takes only shots
not done; Re-run failed sets not done the failed, stopped and aborted shots, and
those awaiting multishot whose last pass failed. A routine's status icon gains
`'aborted'` beside `'done'` and `'error'`, and shows none for `'not run'`. At
the end of each phase lyse puts the outcomes in the shot's row as `('lyse',
<routine>)` columns. Saved dataframes retain them; an independent reload from a
shot file does not recover execution status. Before a new attempt's single-shot
outcomes are recorded, previous outcome cells for that row are cleared, so a
removed routine leaves no stale failure; a pass replaces its previous multishot
outcomes beside the single-shot outcomes. Existing shot-file data is neither
changed nor migrated by status recording. A row whose file is
gone has the deleted state, which keeps it out of analysis, Re-run selected,
Re-run failed and every pass.

**E. The first plugin** (10–11). `lyse/plugins/delete_repeated_shots/` has no
per-shot memory. At `analysis_done`, on the analysis thread, it snapshots the
row and its receipt provenance on the GUI thread. If its source is runmanager,
every outcome is `'done'`, and its `sequence_index` is −1 or its `run repeat`
1 or more, it deletes the file with
`labscript_utils.h5_lock.delete_shot_file` and marks the row deleted, without
the warning. An `OSError` is reported in the output box, and the row stays as it
is.

**F. labscript-agents and labscript-optimization** (13). Each package registers
a plugin under the `lyse.plugins` entry point and handles `analysis_done`. Each
keeps its embedded panel in lyse's GUI thread. The analysis callback runs on
lyse's analysis thread and reads the shot's dataframe row through `inmain`.

The optimizer's `Plugin(BasePlugin, WorkersMixin)` creates a shared worker
process in `plugin_setup_complete`. Its configuration, session, learners and
runmanager interface live in that child. `analysis_done` captures the shot's
cost and queues an `observe` state without waiting for the child. That state
yields `queue_work`, then resumes on the GUI thread to apply returned columns
through the shots model's `update_rows` and update the panel. A timer queues
status states to show progress. Start, Pause and Reset use the same state
queue; generation checks discard results from a session that has been reset.
The configuration file is chosen in the panel and its path is saved with
lyse's configuration. During `close()`, the plugin stops its timer and state
machine on the GUI thread, then finalises its worker on lyse's background
plugin-close thread.

The agent plugin uses threads rather than G's worker processes. Its MCP server
runs an asynchronous loop on a background thread; tool calls execute on worker
threads, and a separate batch watcher checks submissions. `analysis_done`
publishes a result event for an engaged shot and wakes the watcher. Activity
updates reach the panel through `inmain_later`. Its `close()` stops the link
indicator, signals the batch watcher to stop, and stops the MCP server.

**G. Plugin workers** (14). `labscript_utils.workers` implements both the parent
and child used by BLACS and the optimizer, using zprocess's processes, queues,
heartbeat and locks:

- **Parent setup:** a host calls `WorkersMixin._init_workers(app_name,
  host_name, ...)` on the GUI thread, then `create_worker(name, WorkerClass,
  workerargs)`. The class or its import path identifies the child. Creation
  queues worker initialisation ahead of ordinary states; the shared job thread
  starts the process through `ProcessTree.instance()` and runs `init()` first.
  A plugin sets `output_redirection_port` from the setup dict's
  `output_box.port`, keeping child output in lyse's Output tab.
- **States and results:** `StateQueue`, `StateMachine` and `define_state` are
  shared. A method decorated with `define_state` is queued under the host's
  mode rules and runs on the GUI thread. It yields
  `self.queue_work(worker_name, method_name, *args, **kwargs)`. One job thread
  per host checks pickling, sends the request and waits for replies; the GUI
  thread resumes the generator with the result. The host receives job failures
  and fatal communication errors through `_worker_job_finished` and
  `_worker_fatal_error`.
- **Parent close and restart:** `close_workers()` stops the state machine and
  interrupts communication on the GUI thread. `finalise_workers()` runs on a
  thread that may block: it interrupts startup, joins the job thread, sends
  `'quit'`, and ends children using the shared `WORKER_*_TIMEOUT` bounds. It
  returns whether every exit was confirmed and retains unconfirmed workers.
  A host must not reinitialise or start replacements until finalisation succeeds.
- **Child:** `Worker(Process)` runs jobs one at a time on its main thread and
  receives requests on a separate thread. Subclasses define job methods and may
  override `init()` and `shutdown()`. The shared `run()` sets up logging, the
  excepthook, zlock identity and workerarg attributes. A request receives an
  acknowledgement and completion reply, or one failed reply if its method is
  missing. A job's exception, `KeyboardInterrupt` or `SystemExit` is reported
  as a failed job. Quit and parent loss interrupt a running job and call
  `shutdown()` at most once, without interrupting shutdown. Exit waits for open
  HDF5 writes under the kill lock, subject to the shared timeout bounds.

BLACS's `Tab` uses `WorkersMixin`; its `Worker` subclasses the shared child.
Device-specific mode validation, GUI error display, connection-table workerargs,
logging identity, remote-client configuration and buffered-shot path conversion
remain in BLACS. lyse's analysis-routine workers have a separate single-reply
protocol and do not use this parent state machine.

| Requirement | Part |
| --- | --- |
| 1–3 | A |
| 4, 6 | B |
| 5 | C |
| 7–9, 12 | C, D |
| 10–11 | E |
| 13 | F, in labscript-agents and labscript-optimization |
| 14 | G, in labscript-utils |

## Suite integration

- **runmanager:** its analysis submission passes `add_shot(path,
  source='runmanager')`.
- **labscript-utils:** supplies the shared parent and child (G),
  `labscript_utils.h5_lock.delete_shot_file(path)`, which E and BLACS's
  `delete_repeated_shots` both call, and the `[lyse/plugins]` section documented
  beside `[blacs/plugins]`, with lyse's `delete_repeated_shots` off by default.
- **BLACS:** its `Tab` and `Worker` use the shared parent and child (G), and its
  `delete_repeated_shots` calls `delete_shot_file`.
- **labscript-agents:** supplies the Agent panel and MCP tools for the remote
  interfaces; its server, tool execution and batch watcher use threads in lyse's
  process (F).
- **labscript-optimization:** supplies the Optimizer panel, stores its chosen
  configuration path with lyse's settings, and runs its session, learners and
  runmanager interface in the shared worker process (F, G).
