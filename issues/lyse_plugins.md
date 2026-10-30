# Remote control, plugins and robust analysis

Ian's request: lyse needs a remote interface like runmanager's, so that
labscript-agents can drive it; plugins like BLACS's, called when a shot arrives
and when its analysis ends; analysis that a failing routine does not stop; and,
as the first plugin, one that deletes analysed default and repeated shots.
labscript-agents and labscript-optimization become such plugins. Items marked
**Q** are inferences for Ian to confirm.

## Requirements

### Remote interface

1. A client on another process or computer can, for each routine box: list its
   routines in order with whether each is active and how its last run ended;
   add classic scripts and GUI routine folders; remove, activate, deactivate,
   reorder and restart routines.
2. A client can control the rest of the interface: pause and resume analysis,
   Abort, Run the multishot routines, re-run given shots, re-run the failed
   shots, add and remove shots, read the dataframe (as `lyse.data()` does now),
   and set the stop-on-failure option (7). **Q:** also save and load lyse's
   configuration and the dataframe?
3. Each remote command does what the matching button or menu item does, with
   the same result in the window.

### Plugins

4. A lyse plugin is written as a BLACS plugin is, in the suite's one plugin
   language, and is loaded or not as BLACS's are: a flag per plugin in the
   labconfig, in a `[lyse/plugins]` section.
5. lyse offers every loaded plugin a call at each of these points, and each
   plugin chooses what it does at each:
   - **shot received**: a shot has been added to the shots table. **Q:** every
     shot added (from runmanager, the add-shots dialog, Load dataframe), or only
     runmanager's?
   - **single-shot done**: a shot's single-shot routines have ended;
   - **multishot done**: a multishot pass has ended;
   - **analysis done**: all analysis of a shot has ended: its single-shot
     routines and the multishot pass that includes it, whether each succeeded
     or failed. If no multishot routine is active, this follows single-shot
     done. A shot analysed again gets the calls again.

   Each call after analysis says how every routine involved ended.
6. A plugin can put its controls in defined places of lyse's window (the menu
   bar and named areas, as BLACS's plugins do) and can open windows of its own.
   A plugin's settings and window layout are saved and restored with lyse's
   configuration.

### Robust analysis

7. A failing routine never stops lyse: every call point of (5) still happens
   for that shot, and the next shot's analysis starts. An option, *stop a shot
   at its first failure*, chooses whether the shot's later single-shot routines
   run after one fails. **Q:** off or on by default?
8. lyse records how each routine ended for each shot it ran on: success, error
   (an exception, or `sys.exit` with a failure status), aborted, or not run
   (skipped by the option of 7). **Q:** in the shot file and the dataframe both
   (the design below), or one of them? And for a multishot routine, recorded
   on every shot of its pass?
9. A shot on which any routine failed shows as failed in the shots table. A
   **Re-run failed** button, and its remote command, analyses all failed shots
   again, in order, without selecting them.

### The first plugin

10. After all analysis of a shot (5, analysis done), if every routine on it
    succeeded and the shot is a default shot or a repeat, the plugin deletes
    the shot file, under the suite's file lock. The shot's row stays in the
    table with its results, so a run of repeated shots still leaves its record,
    such as atom numbers. A default shot is recognised by an attribute that
    runmanager writes in it (below); a repeat by its `run repeat` attribute, 1
    or more, which BLACS writes with the `_rep` suffix. **Q:** is `run repeat`
    enough for repeats, or should BLACS also write a separate flag?
11. The plugin is on or off by its labconfig flag (4).
12. A row whose file is gone, deleted by the plugin or otherwise, is kept and
    shown as deleted in the status column. It is never analysed again: Re-run
    selected and Re-run failed skip it, unless its file is back, and no
    multishot pass offers it in `lyse.paths` again. `lyse.data()` and Save
    dataframe include it with its results; Load dataframe restores it as
    deleted; Remove takes it out of the table like any row. A file the plugin
    deleted brings no warning; one that vanished otherwise brings lyse's
    present warning. **Q:** should the dataframe also say that a shot's file is
    gone, for routines that open shot files?

### labscript-agents and labscript-optimization

13. Each becomes a lyse plugin, called at analysis done (5) for every shot.
    labscript-agents publishes its `shot_results` event for the shots it
    engaged; labscript-optimization reads the shot's cost and feeds its
    session. Each keeps its own window, with its own on/off control there.
    **Q:** their GUI-routine forms are removed once the plugins work?

## Architecture

**A. Remote interface** (1–3). `LyseServer` (`communication.py`) gains a
`handle_<command>` per command, and `LyseClient` (`client.py`) the matching
method, as runmanager's server and client do (labscript-style rule 11). Each
handler calls the code path the button uses, on the GUI thread through
`inmain`. A box is named `'singleshot'` or `'multishot'`. Proposed commands:
`get_routines`, `add_routines`, `remove_routines`, `set_routines_active`,
`set_routine_order`, `restart_routines`; `get_analysis_paused`,
`set_analysis_paused`, `abort`, `run_multishot`, `rerun_shots`, `rerun_failed`,
`remove_shots`, `get_shot_status`, `get_stop_on_failure`,
`set_stop_on_failure`; `add_shot` and `get_dataframe` stay.

**B. Plugin host** (4, 6, 11). `lyse/plugins/__init__.py` builds a
`labscript_utils.plugins.PluginManager` exactly as `blacs/plugins/__init__.py`
does: plugin folders in `lyse/plugins/`, installed packages through the entry
point group `lyse.plugins`, flags in the labconfig's `[lyse/plugins]`. Each
plugin's `plugin_setup_complete` gets a dict: the app, its `ui`, the labconfig,
the shots model and the two routine boxes. The menu bar is a `MenuContext`;
the named areas are empty layouts in lyse's `.ui` files that plugins add
widgets to, as BLACS's do; a plugin's own window is its own widget, shown from
a menu entry it contributes. Plugin save data goes in lyse's configuration
file. Nothing new is needed in labscript-utils.

**C. Call points** (5). The filebox fires `shot_received(path)` from the
incoming loop once the shot is in the table, and from the analysis loop
`singleshot_done(path, outcomes)`, `multishot_done(paths, outcomes)` and, after
each multishot pass, `analysis_done(path, outcomes)` for every shot of the pass.
`outcomes` maps each routine's name to how it ended (8). Handlers run on the
analysis thread: a slow one starts its own thread, and touches the window
through `inmain`. A handler's exception is logged and analysis goes on
(`fire_event` does that now).

**D. Outcomes and failures** (7–9). The routine box returns each routine's
outcome with its results. With the option off it runs every routine; the
filebox no longer pauses on a failure. The shots model keeps a failed flag per
shot, drawn in the status column; Re-run failed marks those shots not done.
After a shot's single-shot routines lyse writes their outcomes into the shot
file in one open, as attributes of a `results/lyse` group, one per routine, so
the dataframe shows them as `('lyse', <routine>)` columns, live and on reload.
A multishot routine's outcome goes on each shot of its pass the same way.
A row whose file is gone has the shots model's present deleted state, which
already keeps it out of analysis and Re-run selected. Re-run failed and a
failed pass's offer of its shots skip such a row too, and Load dataframe marks
a row whose file is missing as deleted (12).

**E. The first plugin** (10–11). `lyse/plugins/delete_warmup_shots/`, after the
BLACS plugin `delete_repeated_shots`: on `analysis_done`, if every outcome is a
success and the shot is a default or a repeat, it queues the file for a thread
that deletes it under `Lock(path_to_agnostic(path))` and marks its row
deleted, without the warning (12).

**F. labscript-agents and labscript-optimization** (13). Each package registers
a plugin under the `lyse.plugins` entry point and handles `analysis_done`.
They run in lyse's process, as BLACS's plugins run in BLACS's, so their slow
work stays on their own threads, as now. They read the dataframe through the
same selection `lyse.data()` uses, and labscript-optimization adds its columns
through the shots model's `update_rows`. The packages' owners port them.

| Requirement | Part |
| --- | --- |
| 1–3 | A |
| 4, 6 | B |
| 5 | C |
| 7–9, 12 | D |
| 10–11 | E, with runmanager's attribute |
| 13 | F, in labscript-agents and labscript-optimization |

## Needed elsewhere

- **runmanager:** write a root attribute `default_shot = True` in every default
  shot's file (the file whose `sequence_index` is −1); other shots need none.
  lyse reads it into the dataframe, as it reads `run repeat`.
- **labscript-utils:** document the `[lyse/plugins]` section beside
  `[blacs/plugins]` (`docs/source/labconfig.rst`, `example.toml`).
- **labscript-agents, labscript-optimization:** their plugins (F), once A–C
  exist.
- **BLACS:** **Q:** keep its `delete_repeated_shots`, which keeps the last N
  repeats regardless of analysis, beside lyse's plugin?
