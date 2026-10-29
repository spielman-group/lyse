# lyse deep pass

## Upstream stage

Upstream's `master..upstream/master`, to `4c6ae2b`: 109 commits, 68 of them not
merges. Ported by hand onto `CodeHealth`, each commit naming its upstream source.
Range: `8c04e38..4c7346b`.

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

### Waiting on another repo

| Upstream | What | Waits on |
| --- | --- | --- |
| 8af660f | `Run.get_globals` returns Python values, documented in the docs and `NEWS.md` | labscript-utils: `get_shot_globals`'s conversion on an open group |
| 455f19b, 197dbad | Workers start concurrently | zprocess 3's broker guard |
| 23ff55a, 07866b2 | A clear exit if lyse's port is in use | zprocess 3's `port_in_use`, in labscript-utils' `ZMQServer` |
| 8d7a297, b8346c9 | The exit crash workaround | labscript-utils' `exec_qapplication` |
| 3a0888c, 2b5d616 | The suite's font and style | labscript-utils' `setup_application` |

### For Ian later

- Large-table speed: 4d30a1e, 10d9022, 79f9db9, b33f1c6, 8f4d140, 0bb3acd,
  ed71cc7, 4954e44, 4edeed4, 4394eb3, f347bca.
- Attributes of the shot's `data` group in the dataframe: 6066df4, 001df5f,
  baffcac, b8a55d0, ff31a48, 0b010a9.
- `lyse.data(columns=)`: de837f5.
- No multishot pass while analysis is paused: 9cff0f3.

### Not adopted

- Conflicting with the fork: 40370bd drops `get_globals`' `group` argument, which
  users' scripts pass; 6003d7c and 9c52248 stop pausing on a multishot error,
  which breaks `lyse.paths`' retry; f424513's Abort interrupts the main thread,
  where GUI routines do not run; 0625332 serves the command-line shot path the
  fork removed.
- Not needed: b956ecf, 74e6127, 4e593d4, 28675dc, d2bc831, 03e3c95, 9c573dd,
  0c73f77, 25cd0bf.
- Already in `CodeHealth`: 50335c6, 2bf63cb, 3723c9b, feb7540, f9e6e72.
