"""Settings that have to be in place before the modules that read them are
imported.

Running the tests is not supposed to put anything on the screen of whoever runs
them, in either of the two ways this suite manages it.

``QT_QPA_PLATFORM`` covers windows. Nothing here shows one today:
``test_widgets.py`` imports ``lyse.widgets``, a leaf module that builds no
``QApplication`` -- unlike ``lyse.__main__``, which shows a splash at module
scope and hides it only under ``if __name__ == '__main__'``. This is set for the
test after that one. A test asserting on geometry or pixels has to call
``.show()``, because Qt does not lay out a widget that was never shown, and the
fix for such a test is not to stop showing the window. Rendering offscreen lets
it go on showing a real one without a window appearing on someone's desk.

``LABSCRIPT_NO_ERROR_DIALOG`` covers error dialogs. ``labscript_utils.excepthook``
replaces ``sys.excepthook`` at import, and every unhandled exception then spawns
a tkinter subprocess window. An ordinary test failure does not reach it -- pytest
catches what the test body raises. What does reach it is an exception PyQt
diverts out of a virtual method, which it hands to ``sys.excepthook`` before
going on painting: one window per repaint, for as long as the repaints last.
``test_painting_through_the_view_reaches_no_excepthook`` drives exactly that
path. Exceptions are still logged and still reach stderr with this set, so
nothing diagnostic is lost.

Both are set here rather than in a fixture because each has to take effect
before the module that reads it is imported -- qtutils imports Qt, and
``labscript_utils.excepthook`` evaluates its variable at module scope -- and
pytest imports conftest before the test modules.

``setdefault`` leaves an explicit setting alone, so exporting either variable
yourself overrides what is set here: ``QT_QPA_PLATFORM`` to watch a test drive a
real window, ``LABSCRIPT_NO_ERROR_DIALOG=0`` to let the error dialog through.
``''``, ``'0'``, ``'false'``, ``'no'`` and ``'off'`` leave the dialog enabled,
as does leaving the variable unset; anything else suppresses it, ignoring
surrounding whitespace and case.

A test that wants the real dialog can leave the environment alone and assign to
``labscript_utils.excepthook.NO_ERROR_DIALOG``, which the handler reads where it
uses it rather than at the point it is set above. Such a test should stub
``subprocess.Popen`` so it cannot spawn windows.
"""
import os

os.environ.setdefault('QT_QPA_PLATFORM', 'offscreen')
os.environ.setdefault('LABSCRIPT_NO_ERROR_DIALOG', '1')
