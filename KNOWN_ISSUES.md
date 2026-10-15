# Known issues

- `routines.py`, `AnalysisRoutine.do_analysis`: a worker that dies during an analysis, or just before a shot is sent to it, leaves the analysis waiting for a reply that never comes, and the routine box analyses nothing more until the routine is restarted or removed.
- `widgets.py`, `ItemDelegate.paint`: on Windows 10 the status column's progress-bar animation runs on the first shot, not the one being analysed, since the delegate leaves the option's `styleObject` unset. Left for the move to Windows 11.
