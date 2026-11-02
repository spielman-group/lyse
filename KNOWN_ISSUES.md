# Known issues

- `widgets.py`, `ItemDelegate.paint`: on Windows 10 the status column's progress-bar animation runs on the first shot, not the one being analysed, since the delegate leaves the option's `styleObject` unset. Left for the move to Windows 11.
- `routines.py`, `RoutineBox.do_analysis`: the next routine starts as soon as the previous one replies, before the filebox has applied its results, so it can read the table through `lyse.data()` without them.
- `routines.py`, `RoutineBox.do_analysis`: distinct routine paths with the same basename share an outcome key, so a later success can overwrite an earlier failure and incorrectly permit successful completion or deletion.
