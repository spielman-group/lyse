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
   the shots table, and so in the dataframe, and written into the shot file; a
   multishot routine's is on every shot of its pass. Analysing a shot again
   replaces its record.
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
    session. Each keeps its own window, with its own on/off control there.
    This is a full port: nothing of their GUI-routine forms remains, no
    `lyse.Routine` subclass and no `.lyse` folder in their examples. Each keeps
    its window and its call points in lyse's process and does its work in a
    worker of its own (14).

### Plugin workers

14. Any plugin can run its work in worker processes of its own, as a BLACS
    device tab does, so that a crash or slow work stays out of lyse. The plugin
    starts its workers, sends them jobs, restarts and ends them; sending a job
    does not block the sender, and the job's result reaches the plugin when the
    worker has done it. lyse ends a plugin's workers when it closes the plugin.
    The first plugin (10) needs no worker and has none.

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

**B. Plugin host** (4, 6, 11). `lyse/plugins/__init__.py` builds a
`labscript_utils.plugins.PluginManager` as `blacs/plugins/__init__.py` does:
plugin folders in `lyse/plugins/`, installed packages through the entry point
group `lyse.plugins`, flags in the labconfig's `[lyse/plugins]`. Menu items come
from `get_menu_contributions`, rendered by a `MenuContext` on which lyse
registers its menu bar as the `'menubar'` location, as BLACS does. Widgets come
from `get_ui_contributions` through a context lyse registers as `'areas'`, a
class with `add(plugin_name, contribution, data)` after BLACS's notifications:
`{'context': 'areas', 'area': name, 'widget': widget}` adds the widget to the
end of a named area, `'analysis_controls'` beside Pause and Abort or
`'shots_toolbar'`. A plugin's window is its own widget, shown from a menu item
it contributes, and its geometry is part of its `get_save_data`. The setup dict,
keyed as BLACS's is, holds the labconfig, the `ui`, the shots model and the two
routine boxes. At start-up lyse builds its boxes, whose threads wait for shots,
then instantiates the plugins, restores their data from the configuration it
autoloads, sets up the contexts and calls `plugin_setup_complete`, and only then
starts its server, so no call precedes setup. Loading a configuration later
restores plugin data again, as BLACS does. Plugins start their workers (G) in
`plugin_setup_complete`, before the server. At quit, after the save prompt, lyse
fires no more calls, starts no more analysis, and ends the routines' workers,
whose locks a pending deletion may wait on; then it calls the plugins' `close()`
from a thread of its own while its GUI thread keeps serving, so that a `close()`
may wait for its workers to end (G) and for its own threads, which may need the
GUI thread. Plugin data is saved in lyse's configuration. Nothing new is needed
in labscript-utils.

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
unless Run, the button or the remote command, starts the pass. A shot removed
during its attempt gets no further call. `shot_received(path, source)`, with
`source` one of `'runmanager'`, `'remote'`, `'dialog'` and `'dataframe'`, fires
on the GUI thread inside the shots model's `add_files`, once the row is in, so
before the analysis loop, which asks the GUI thread for its next shot, can take
it. Each path carries its source from where it was queued, the server or the
add-shots dialog, through the incoming loop's merging to `add_files`. Load
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
the end of each phase lyse puts the outcomes in the shot's row, as `('lyse',
<routine>)` columns, and writes them into the shot file, in one open under the
h5 lock, as attributes of a `results/lyse` group, which the dataframe reads on
reload. A routine may write the same group, as routines can any group. Before it
writes a new attempt's single-shot outcomes, lyse removes the attributes and
columns its previous outcomes put there for that shot, and no others, so a
removed routine leaves no stale failure; a pass replaces the multishot outcomes
it wrote before in the same way. A failed write, of a missing or read-only file,
is reported in the output box; the row keeps the outcomes. A row whose file is
gone has the deleted state, which keeps it out of analysis, Re-run selected,
Re-run failed and every pass.

**E. The first plugin** (10–11).
`lyse/plugins/delete_default_and_repeated_shots/` notes at `shot_received` the
paths runmanager sends. At `analysis_done`, on the analysis thread, if the shot
is one of them, every outcome is `'done'`, and its file's `sequence_index` is −1
or its `run repeat` 1 or more, it deletes the file with
`labscript_utils.h5_lock.delete_shot_file` and marks the row deleted, without
the warning. An `OSError` is reported in the output box, and the row stays as it
is.

**F. labscript-agents and labscript-optimization** (13). Each package registers
a plugin under the `lyse.plugins` entry point and handles `analysis_done`. Each
keeps its window and its handlers in lyse's process and runs its work in a
worker (G): labscript-agents its MCP server, labscript-optimization its session.
`analysis_done` sends the worker a job with the shot and returns at once; the
job's result, such as the optimizer's columns, comes back to the plugin, which
adds them through the shots model's `update_rows`. The plugin reads the
dataframe for its jobs through the same selection `lyse.data()` uses. A window
shows its worker's live status, such as the optimizer's progress or the agent's
activity during a tool call, by asking for it with a job on a timer. A Future's
callbacks run on the worker's thread, so every window update goes to lyse's GUI
thread through `inmain_later`. Their `lyse.Routine` subclasses and `.lyse`
example folders go. The packages' owners port them (below).

**G. Plugin workers** (14). The capability is labscript-utils', for plugins in
general, in a new module `labscript_utils.workers`, modelled on BLACS's tab
workers (`blacs/tab_base_classes.py`), whose code splits into a shared part and
BLACS's own:

- Shared, the worker side: `Worker(Process)` with `init()`, `shutdown()` and job
  methods; its `run`, which sets up logging, the excepthook and the zlock name
  and makes each workerarg an attribute; its `mainloop`, which runs jobs in the
  main thread, receives on a thread of its own and replies to a job twice, the
  method found, then success, message and results, or once when the method is
  missing; and its quit: the `'quit'` message, the interrupt of a running job,
  `shutdown()`, `kill_lock.exit_when_released` and the heartbeat's parent death.
- Shared, the parent side, a mixin, `WorkersMixin`: `create_worker(name,
  WorkerClass, workerargs)`, the class or its import path, through
  `ProcessTree.instance()` with the host's output box; sending a job and reading
  its replies; `end_workers()`, with `'quit'` and then a kill after
  `WORKER_QUIT_TIMEOUT`; and `restart_worker(name)`, an end, then a create. The
  `WORKER_*_TIMEOUT` constants move into the module.
- BLACS's own, which stays in `Tab` and its `Worker` subclass: the state
  machine (`define_state`, the event queue, `queue_work` yielded from a state),
  the connection table's workerargs, remote process clients, the tab's display
  of its workers, and `_transition_to_buffered`'s path conversion.

What plugins need first: the shared worker base and `WorkersMixin`, with
`submit_job(worker_name, method_name, *args, **kwargs)`, which only queues the
job and returns a `concurrent.futures.Future`, in place of the state machine.
One thread per worker takes its jobs in order, sends each and reads its replies,
and completes its Future, at once with the error when the lookup fails; when the
worker exits, ends or restarts, that thread fails the futures of its running and
queued jobs. Other needs: workers started by `create_worker` itself, where `Tab`
starts them at its first state, with `init()` queued as their first job, before
any other; a failed `init()` fails its Future, which `create_worker` returns,
and ends the worker, failing the jobs queued behind it; their output in the
host's output box, whose port lyse puts in the setup dict; and `end_workers()`
from the plugin's `close()`. The shared code keeps what BLACS's
`DESIGN_TAB_STATE_MACHINE.md` fixes (R8–R11, F3, F4, F14): `shutdown()` called
at most once and never interrupted, the quit skipping it once a shutdown job has
started; a job that starts after a quit stopped at once; a workerarg refused
only for a name `Worker` or zprocess's `Process` uses; the `'quit'` message; the
job tuple `(funcname, args, kwargs)` checked for pickling before it is sent; two
replies per job, or one for a missing method; the timeouts, `exit_when_released`
and `on_parent_death`. Its `run` takes the app's name, for `setup_logging` and
for the logger's name, which is also the zlock process name, where BLACS's says
`'BLACS.<device>_<worker>.worker'`. BLACS's tab pass is finished (blacs
`Development` `6ff09fb`), so it can be cut now; the clean-up tests of blacs's
`tests/test_state_machine.py`, which drive real workers, move with it. Moving
`Tab` onto it is a later step; lyse's routine workers, whose protocol is one
reply to `[task, data]`, stay apart for now and share only the timeout
constants; `labscript_optimization.worker.Worker`, a thread that returns
futures, retires once the optimizer's session runs in a plugin worker.

| Requirement | Part |
| --- | --- |
| 1–3 | A |
| 4, 6 | B |
| 5 | C |
| 7–9, 12 | C, D |
| 10–11 | E |
| 13 | F, in labscript-agents and labscript-optimization |
| 14 | G, in labscript-utils |

## Needed elsewhere

- **runmanager:** its analysis submission passes `add_shot(path,
  source='runmanager')`.
- **labscript-utils:** done on its `LysePlugins` branch:
  `labscript_utils.h5_lock.delete_shot_file(path)`, which E and BLACS's
  `delete_repeated_shots` both call, and the `[lyse/plugins]` section documented
  beside `[blacs/plugins]`, with `delete_default_and_repeated_shots` off by
  default. To do: the plugin workers (G).
- **BLACS:** its `delete_repeated_shots` stays, calling `delete_shot_file`. Later,
  its `Tab` and `Worker` move onto the shared worker parts (G).
- **labscript-agents**, once A–C and G exist: its plugin (F); ToolSpec bindings for the
  new lyse commands; its docs' line that it has no control of lyse goes; its MCP
  server in its plugin's worker, with the clean-up that ran when its lyse worker
  exited in that worker's `shutdown()`.
- **labscript-optimization**, once A–C and G exist: its plugin (F); a field in its
  window holding its configuration file's path, chosen with a file picker and
  saved and restored with the plugin's save data in lyse's configuration, so
  nothing is relative to a routine folder; its session in its plugin's worker;
  the hand-over from `analysis_done` a job sent without blocking, in place of
  the 2 s wait, its late reply delivered as the job's result; the clean-up that
  ran when its lyse worker exited in that worker's `shutdown()`.
