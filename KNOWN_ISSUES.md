# Known issues

- `routines.py`, `AnalysisRoutine.do_analysis`: a worker that dies during an analysis, or just before a shot is sent to it, leaves the analysis waiting for a reply that never comes, and the routine box analyses nothing more until the routine is restarted or removed.
- `widgets.py`, `ItemDelegate.paint`: on Windows 10 the status column's progress-bar animation runs on the first shot, not the one being analysed, since the delegate leaves the option's `styleObject` unset. Left for the move to Windows 11.
- `routines.py`, `RoutineBox.do_analysis`: the next routine starts as soon as the previous one replies, before the filebox has applied its results, so it can read the table through `lyse.data()` without them.
