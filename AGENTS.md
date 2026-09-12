# Working in lyse

## Tests must not invoke the application

`lyse/__main__.py` builds a `Splash` and calls `.show()` at module scope, and
`Splash.__init__` creates the `QApplication`. `splash.hide()` runs only inside
`if __name__ == '__main__'`. So **importing `lyse.__main__` puts a banner on the
user's screen and leaves it there** — during a test run, during a REPL session,
during anything.

The rule, in order of preference:

1. **Import the leaf module.** `tests/test_widgets.py` imports `lyse.widgets`
   and triggers nothing. This is the case to copy.
2. **If that is not possible**, load the module by path and stub its
   dependencies in `sys.modules`, restoring them in a `finally`.
3. **If a test must borrow from `__main__`** — to exercise a real method rather
   than a description of it — stub `labscript_utils.splash` in `sys.modules`
   *before* the import. A fake `Splash` whose `__init__`, `show`, `hide` and
   `update_text` do nothing, and a `get_qapplication` returning `None`, is
   enough: no `QApplication` is created and the real methods are still borrowed.

A test that genuinely renders — geometry or pixel assertions — must still call
`.show()`, because Qt does not lay out or paint an unshown widget. That is safe
here: `tests/conftest.py` renders offscreen, so a shown window never reaches
the screen.

## Running the tests

From inside this repository, never from the workspace root — a workspace-root
cwd shadows the installed packages:

    python -m pytest tests -q

Nothing else belongs on that line. `tests/conftest.py` sets both
`QT_QPA_PLATFORM=offscreen` and `LABSCRIPT_NO_ERROR_DIALOG=1`, each before the
module that reads it is imported, so a test run neither opens a window nor
spawns a tkinter error dialog of its own accord.

Both use `setdefault`, so exporting either yourself overrides what the conftest
sets — `QT_QPA_PLATFORM` to watch a test drive a real window,
`LABSCRIPT_NO_ERROR_DIALOG=0` to get the error dialog back.

`=0` only started meaning that with the labscript-utils commit "Let
LABSCRIPT_NO_ERROR_DIALOG=0 mean what it looks like" (`ae73495` at the time of
writing; the subject outlives the hash). Before it the variable was read as
`bool(os.environ.get(...))`, so `=0` suppressed the dialog exactly as `=1` did. A comment anywhere in the suite still saying so is stale.

A test of the dialog itself can leave the environment alone and assign to
`labscript_utils.excepthook.NO_ERROR_DIALOG`, which the excepthook reads where
it uses it.

Note that lyse has **no CI that runs these tests** — `.github/workflows` is
release-only. They run when someone runs them.

## More

The workspace `AGENTS.md`, one directory up, carries the longer reasoning and
the conventions shared across the suite. This file exists because work often
happens inside one repository with no view of that one.
