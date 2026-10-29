# lyse deep pass

## Upstream stage

Upstream's `master..upstream/master`, to `4c6ae2b`: 109 commits, 68 of them not
merges. Ported by hand onto `CodeHealth`, each commit naming its upstream source.
Range: `8c04e38..5d6a338`.

| Upstream | What | Commit |
| --- | --- | --- |
| 9766125 | `sys.exit()` in a routine replies | d566e36 |
| fb3195a | A routine's traceback starts at the routine | 494b23a |
| 6c8312d | No kill lock across a whole run | f8af88c |
| 00a26a1, b35294c | A result of another dtype no longer stops analysis under pandas 3 | 644ced9 |
| 061ce1f | New columns made with `.loc` | 8d4f46f |
| 9b678b9 | Integer-indexing headers from a float column | 6d60c0c |
| 1dc53ea | An unreadable shot is skipped, not its batch | 0a2fceb |
| fed218a, 87703f8 | Shots stamped in a daylight saving transition hour load | 8d24890 |
| af6977d | A shot not yet run is not loaded | bfa817d |
| 8a6a5d5 | `lyse.spinning_top` | 0c90b60 |
| 74f1968 | The server starts once the filebox exists | d0b8212 |
| d68e43b | A worker starts without `autoload_config_file` | e7ace3b |
| 740dd2b | The Qt6 progress bar fills horizontally | 8a44abf |
| ae38219 | Units and expansion subgroups matched by name | bcb0b62 |
| ce9d019 | Load dataframe adds no index column | f32698e |
| 9ff5102 | `lyse.data()` and Save dataframe infer column types | 7d00a47 |
| 12cbb2c | Python floor, set to the suite's 3.13 | 51bd3c2 |
| 1beb61c, 9c71b2e | High-DPI icons; qtutils 4.4.1 | 4c7346b |
| 8af660f | `Run.get_globals` returns Python values, through labscript-utils' `get_group_globals`; in `NEWS.md` | 541a929 |
| 455f19b, 197dbad | Workers start together, across both boxes | c88c079 |
| 8d7a297 | The output box's thread stops before lyse exits | 6a5d81f |
| 07866b2 | zprocess floor, 3.0.1, for the concurrent starts | 8d9ef86 |
| 23ff55a | A second lyse exits with a message when its port is in use, from labscript-utils' `ZMQServer` | 2325a16 |
| b8346c9 | The PyQt exit crash workaround is labscript-utils' `exec_qapplication`; the output box stops before it | 72c5efb |
| 6066df4, 001df5f, baffcac, b8a55d0, ff31a48, 0b010a9 | The shot's `data` groups in the dataframe; collisions reported; live results match a reload | 42dd396 |
| de837f5 | `lyse.data(columns=)` | 103d721 |
| 40370bd | `get_globals` and `globals_diff` take no `group` | 09e3762 |
| 28675dc | A classic routine compiled only when its source changes | 47cdb4f |
| d2bc831 | The column editor keeps its header items | 436c68e |
| 03e3c95, f347bca | Dialog shots queued in path order, digits by value | 3d866a1 |
| 9c573dd, 0c73f77 | The release workflow | 0955a92 |
| 25cd0bf | The screenshot | 6f5d316 |
| 6003d7c | A multishot routine's error does not pause analysis | 086ee76 |
| b956ecf | The excepthook installed explicitly | 18acd28 |
| 3a0888c, 2b5d616 | The suite's font and style; 20 px rows | bf26f5e |
| f424513 | Abort | 2b790e6 |
| 9c52248 | The routine box says whether a failed pass stopped, in the fork's form | e0e13dd |
| 4e593d4 | Logging arguments passed lazily | 5d6a338 |

`NEWS.md`'s Unreleased section, and the docs where they describe them, say what
changes for users and scripts.

### Designed, for Ian's review

The large-table set: 4d30a1e, 10d9022, 79f9db9, b33f1c6, 8f4d140, 0bb3acd,
ed71cc7, 4954e44, 4edeed4, 4394eb3. Requirements and architecture in
`issues/shots_table_speed.md`; it needs labscript-utils' header view to read
`headerData`.

### Not ported

- 9cff0f3, no multishot pass while analysis is paused: in upstream's form the
  Run multishot button does nothing while paused, until analysis resumes.
- 0625332, a command-line shot path only if it is an HDF5 file: lyse no longer
  takes a shot path from the command line, by Ian's decision in step 2.4.
- 9c52248 in upstream's form: its `done` after a failure would drop the failed
  pass's shots from the next `lyse.paths`; ported in the fork's form instead.

### Already in `CodeHealth`

50335c6, 2bf63cb, 3723c9b, feb7540, f9e6e72, and 74e6127 through labscript-utils'
`open_in_editor`, which now calls `programs.open_file`.
