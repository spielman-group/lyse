## Unreleased

- `Run.get_globals()` returns each global as a Python value, as
  `labscript_utils.shot_utils.get_shot_globals()` does: `None` rather than a null
  HDF5 reference, `bool` rather than `numpy.bool_`, and `str` rather than bytes or a
  numpy string. A script that compared a global with a numpy type or decoded it
  itself may need changing. `globals_diff()` compares these values.


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


