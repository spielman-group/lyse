# Working in lyse

`lyse/__main__.py` builds the `Splash` and `QApplication` at module scope, so
importing it anywhere but a real start puts a banner on the screen that is
never hidden. Import a leaf module instead (`lyse.widgets`). A test that must
borrow from `__main__` uses `import_main_without_splash()` in `tests/test_main_window.py`.

Workspace conventions are in `../AGENTS.md`.
