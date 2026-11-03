# Known issues

- `filebox.py`, `record_outcomes`: clearing every dataframe column under lyse also replaces unrelated user results saved under results/lyse with NaN (source review; not executed).
- `__main__.py`, `load_configuration`: analysis_paused is applied only when true, so loading an unpaused configuration does not unpause an already paused application (source review).
- `filebox.py`, `remove_shots`/`_singleshot_outcomes`: removing a waiting row clears model metadata but leaves its separately owned single-shot outcome snapshot cached (source review).

- `widgets.py`, `ItemDelegate.paint`: on Windows 10 the status column's progress-bar animation runs on the first shot, not the one being analysed, since the delegate leaves the option's `styleObject` unset. Left for the move to Windows 11.
- `routines.py`, `RoutineBox.do_analysis`: the next routine starts as soon as the previous one replies, before the filebox has applied its results, so it can read the table through `lyse.data()` without them.
- `routines.py`, `RoutineBox.do_analysis`: distinct routine paths with the same basename share an outcome key, so a later success can overwrite an earlier failure and incorrectly permit successful completion or deletion.
