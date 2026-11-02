## Unreleased

- Output and plugin panels share nonclosable finger tabs in lyse's main window.
  Plugins contribute a panel, label and optional icon through the `tabs` UI
  context. The selected plugin tab is saved with the configuration; if it is
  unavailable when loading, Output is selected instead.

- A first plugin, `delete_repeated_shots`, off by default: once all
  analysis of a default or repeated shot from runmanager has ended with every
  routine done, it deletes the shot file and keeps its row, with its results.

- A failing routine no longer stops analysis: the routines after it run, analysis
  is not paused, and the next shot is analysed. The option *Stop a shot at its first
  failure* skips a shot's later single-shot routines once one fails. How each
  routine ended, `'done'`, `'error'`, `'aborted'` or `'not run'`, is shown as
  `('lyse', <routine>)` dataframe columns and retained by Save dataframe.
  **Re-run failed** analyses all failed, stopped and aborted shots again. A worker
  that dies during an analysis ends it as an error, where lyse used to wait for it.

- Running shots cannot be removed, including every member of an executing
  multishot batch, until its completion callbacks return. Queued shots and shots
  waiting for a pass or retry remain removable. A shot whose file disappears is
  marked deleted and still reports completion.

- Each shot's status shows the state of its analysis: not done, awaiting the
  multishot routines, done, failed, or deleted. A shot whose single-shot routines
  have run awaits the next multishot pass, and its analysis ends with that pass. A
  failed shot is not analysed again until it is re-run. lyse calls its plugins when a
  shot is received, saying where it came from, when its single-shot routines end,
  when a multishot pass ends, and when all analysis of a shot has ended, with how
  each routine ended. `LyseClient.shot_status` returns the state of shots.

- lyse takes plugins, written as BLACS's are, in the suite's one plugin language:
  folders in `lyse/plugins/` and packages registered under the `lyse.plugins`
  entry point, each loaded if its flag in the labconfig's `[lyse/plugins]`
  section is true. A plugin can add menu items and widgets beside Pause and Abort
  or in the shots' toolbar, and its data is saved with lyse's configuration.

- lyse can be controlled remotely, as runmanager can: `LyseClient` lists, adds,
  removes, activates, reorders and restarts the routines of either box, pauses and
  resumes analysis, aborts, runs the multishot routines, re-runs and removes shots,
  and saves and loads the dataframe, each as its button does.
  `LyseClient.add_shot` takes the shot's source, `'remote'` unless runmanager
  sends it.

- The buttons sit by what they act on: **Run** by the multishot routines, and
  **Re-run selected** (formerly Mark as not done) in the shots' toolbar. **Pause** is
  a toggle. **Run** runs the multishot routines at once even
  while analysis is paused; a pass that is due on its own waits until analysis
  resumes.

- `lyse.Plot`, `lyse.plots`, `lyse.delay_event` and `lyse.figure_to_clipboard` are
  back in the lyse namespace, so a routine that subclasses `lyse.Plot` for
  `register_plot_class` runs again.

- The shots table reads each value from the dataframe as it draws it, so lyse stays
  responsive with tens of thousands of shots. The progress bar of adding shots
  shows only once adding has taken half a second.

- An **Abort** button pauses analysis and interrupts the running routine with
  KeyboardInterrupt, as Ctrl-C would, so its files close and its worker carries on.
  A routine still running five seconds later has its worker restarted. Restarting,
  removing or quitting a worker likewise interrupts its routine, then waits for its
  HDF5 files to close, up to a minute, before the worker exits; lyse kills a worker
  only after 70 seconds, where before it terminated one after 2.

- A multishot routine that raises an exception no longer pauses analysis, and the
  multishot routines after it still run. The next pass offers its shots again in
  `lyse.paths`.

- The dataframe holds the attributes of the groups under a shot's `data` group, as
  it does those under `results`; where both have a group of one name, a results
  attribute wins. A group or global that hides another name, or a shot column such
  as `run time`, is reported in lyse's output box.

- A result saved while lyse runs reaches the shot's row as a reload of the shot
  reads it: a dict as one column per key, a tuple as an array, and only for a group
  directly under `data` or `results`. `save_result(..., save_to_h5=False)` refuses
  any other group, since the result would be kept nowhere.

- `Run.get_globals()` returns each global as a Python value, as
  `labscript_utils.shot_utils.get_shot_globals()` does: `None` rather than a null
  HDF5 reference, `bool` rather than `numpy.bool_`, and `str` rather than bytes or a
  numpy string. A script that compared a global with a numpy type or decoded it
  itself may need changing. `globals_diff()` compares these values.

- `Run.get_globals()`, `globals_diff()` and `Run.globals_diff()` no longer take a
  `group` argument, since a globals group holds only its globals' unevaluated
  expressions; `Run.get_globals_raw(group)` returns those. A script that passed
  `group` must drop it.


## [2.6.0] - 2020-02-17

This release includes six bugfixes, two changes to work with newer versions of a
library, and two enhancements.

- Do not throw a version error about `pandas` v1.0

- Fix bug where lyse would fail to terminate subprocesses upon quit. Normal
  subprocesses would eventually terminate themselves, but this caused lyse to hang
  whilst closing waiting for them.

- Increase the timeout for starting subprocesses to 30 seconds. Slow computers starting
  lyse, or when starting many subprocesses at once, would often cause timeouts which
  necessitated restarting lyse.

- In analysis routine subprocesses, force the multiprocessing module to spawn new
  processes instead of forking. This allows analysis routines to use the multiprocessing
  module, whereas otherwise this causes crashes due to zmq not being fork-safe. This
  only affects Unix, where forking is the default behaviour of the multiprocessing
  module - on Windows this was not an issue since processes were always spawned fresh.

- Expand the allowed datatypes that may be saved/loaded as HDF5 attributes via
  `Run.save_result()` etc to anything supported by
  `labscript_utils.properties.get/set_attribute(s)`. This means that datatypes not
  supported natively by HDF5 are JSON encoded and saved as a string with a prefix
  indicating this.

- Fix bug where lyse's close button was unresponsive if there were no analysis routines.

- Fix incorrect logic for updating the Qt model and dataframe in lyse - it is hoped that
  this will resolve an intermittent issue (issue #45) in which the dataframe and Qt
  model are sometimes not updated after analysis completes. If anyone sees the buggy
  behaviour again, please reopen issue #45 or a new bug report.

- Update to no longer use the deprecated `pandas` `convert_objects()` method, in favour
  of the newer `infer_objects` method. 

- Fix a bug where lyse could crash if loading shots with different numbers of levels in
  the hierarchy of their corresponding dataframes.

- Allow instantiating a `Run()` object from within a function, not just at the global
  scope. The default group that results will be saved to will be the name of the file
  the `Run()` object is instantiated in.


