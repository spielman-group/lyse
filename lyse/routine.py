#####################################################################
#                                                                   #
# /lyse/routine.py                                                  #
#                                                                   #
# Copyright 2026, JQI                                               #
# Author: Ian Spielman                                              #
#                                                                   #
# This file is part of lyse, in the labscript suite                 #
# (see http://labscriptsuite.org), and is licensed under the        #
# Simplified BSD License. See the license.txt file in the root of   #
# the project for the full license.                                 #
#                                                                   #
#####################################################################
"""The Routine base class of GUI routines, and the pieces the worker builds one from."""
import base64
import hashlib
import sys
from collections import namedtuple
from itertools import count
from keyword import iskeyword
from pathlib import Path

# Before matplotlib's Qt backend, which follows the Qt binding that is already imported.
from qtutils import UiLoader
from qtutils.qt import QtCore, QtGui, QtWidgets
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg, NavigationToolbar2QT
from matplotlib.figure import Figure

from labscript_utils.labconfig import load_appconfig, save_appconfig
from labscript_utils.ls_zprocess import get_config
from lyse.utils import LYSE_DIR
import lyse.utils.gui as gui
import lyse.utils.worker as worker
from zprocess.process_tree import OutputInterceptor

CONTROL_VALUE_TYPES = (bool, int, float, str)

# Constructing a UiLoader replaces sys.modules['qtutils.widgets'], which erases the
# custom widgets a routine has registered on its own loader. Windows share this loader,
# made before any routine runs, so that opening one erases nothing.
loader = UiLoader()


def fill_plot_form(form, toolbar, on_copy):
    """Fill a plot form with a toolbar and its canvas; return the toolbar's new Copy action."""
    action = toolbar.addAction(
        QtGui.QIcon(':qtutils/fugue/clipboard--arrow'), 'Copy to clipboard', on_copy)
    action.setToolTip('Copy to clipboard')
    form.verticalLayout_canvas.addWidget(toolbar.canvas)
    form.verticalLayout_navigation_toolbar.addWidget(toolbar)
    return action


class RoutineWindow(gui.ThemedWindow, QtWidgets.QMainWindow):
    """A routine's window, with its Output dock. Closing it hides it."""

    def __init__(self):
        super().__init__()
        loader.load(str(LYSE_DIR / 'user_interface' / 'routine_window.ui'), self)
        self.menu_view.addAction(self.dock_output.toggleViewAction())

    def closeEvent(self, event):
        # Refused, so that Qt does not count the last window closed and quit the application.
        event.ignore()
        self.hide()


class Routine:
    """The base class of a lyse GUI routine.

    A GUI routine is a folder whose name ends in ``.lyse``. Its ``lyse_routine.py``
    defines exactly one subclass. The worker constructs it once, calls `run` for each
    analysis, and calls `close` once as it quits.

    ``__init__()`` and `close` are optional. The worker constructs the object,
    so ``__init__()`` takes no arguments and need not call
    ``super().__init__()``.

    Attributes
    ----------
    output_port : int
        The port of the window's Output box; a child process started with it as its
        ``output_redirection_port`` shows its output there.
    values : namedtuple
        The saved controls' values by objectName; immutable, new for each analysis, empty at first.
    window : QMainWindow
        The routine's window: the controls are its central widget, the figures and output are docks.
    """

    def run(self, path, paths):
        """Analyse the shots of one analysis request; every routine defines it.

        Parameters
        ----------
        path : str or None
            The shot file of a singleshot analysis, as ``lyse.path``.
        paths : list of str or None
            The shot files analysed since the last multishot pass, as
            ``lyse.paths``.
        """
        raise NotImplementedError(f'{type(self).__name__} must define run(path, paths).')

    def close(self):
        """Release the routine's resources, on the GUI thread, once the last `run` returns."""

    def load_ui(self, filename):
        """Load a Qt Designer file as the window's central widget, and return it.

        Parameters
        ----------
        filename : str or Path
            The file, absolute or relative to the routine folder.
        """
        ui = loader.load(str(self._folder / filename))
        self.window.setCentralWidget(ui)
        return ui

    def saved_widgets(self, *widgets):
        """Register controls whose values lyse restores, saves and supplies to `run` as `values`.

        A saved value is restored at once, so set a widget up, its range included, before
        registering it, and connect signals that start work afterwards.

        Parameters
        ----------
        *widgets : QWidget
            Each has a unique objectName that is a Python name, not a keyword and not starting
            with an underscore. Its value is its Qt user property, such as a spin box's value.

        Raises
        ------
        ValueError
            If a name is invalid or shared, or a value is not a number, string or boolean.
        """
        for widget in widgets:
            name = widget.objectName()
            if not name.isidentifier() or iskeyword(name) or name.startswith('_'):
                raise ValueError(f'The objectName {name!r} cannot be an attribute of values.')
            if self._saved_widgets.get(name, widget) is not widget:
                raise ValueError(f'Two saved widgets are named {name!r}.')
            user_property = widget.metaObject().userProperty()
            if not isinstance(user_property.read(widget), CONTROL_VALUE_TYPES):
                raise ValueError(f'The widget {name!r} does not hold a number, string or boolean.')
            saved = self._saved_controls.get(name)
            if saved is not None and not user_property.write(widget, saved):
                print(f'Ignoring {saved!r} for {name!r}: the widget rejects it.', file=sys.stderr)
            self._saved_widgets[name] = widget

    def add_figure(self, name):
        """Return the Figure in the dock called `name`, adding the dock the first time.

        Call on the GUI thread. Repeating a call returns the same Figure and leaves the dock as it
        is. The dock has lyse's navigation toolbar; Ctrl+C copies the Figure while focus is in it.

        Parameters
        ----------
        name : str
            A nonempty string, which identifies the dock in the saved layout.

        Raises
        ------
        ValueError
            If `name` is not a nonempty string.
        """
        if not isinstance(name, str) or not name:
            raise ValueError('A figure name must be a nonempty string.')
        if name not in self._figures:
            form = loader.load(str(LYSE_DIR / 'user_interface' / 'plot_window.ui'))
            figure = Figure()
            toolbar = NavigationToolbar2QT(FigureCanvasQTAgg(figure), form)
            copy_action = fill_plot_form(form, toolbar, lambda: worker.figure_to_clipboard(figure))
            dock = QtWidgets.QDockWidget(name, self.window, objectName=name)
            dock.setWidget(form)
            QtGui.QShortcut(QtGui.QKeySequence.StandardKey.Copy, dock, copy_action.trigger,
                            context=QtCore.Qt.ShortcutContext.WidgetWithChildrenShortcut)
            # A dock added after the layout was restored takes its saved place, if it has one.
            if not self.window.restoreDockWidget(dock):
                self.window.addDockWidget(QtCore.Qt.DockWidgetArea.RightDockWidgetArea, dock)
            self.window.menu_view.addAction(dock.toggleViewAction())
            self._figures[name] = figure
        return self._figures[name]


def routine_class(namespace):
    """Return the one Routine subclass defined in the namespace of lyse_routine.py's module.
    Raise ValueError if it defines none or several."""
    # Classes defined in the module carry its __name__ as __module__; imported classes do not.
    classes = {value for value in namespace.values()
               if isinstance(value, type) and issubclass(value, Routine)
               and value.__module__ == namespace['__name__']}
    if not classes:
        raise ValueError('lyse_routine.py defines no subclass of lyse.Routine.')
    if len(classes) > 1:
        names = ', '.join(sorted(cls.__name__ for cls in classes))
        raise ValueError(
            f'lyse_routine.py defines more than one subclass of lyse.Routine: {names}. '
            'A shared base class belongs in an imported module.')
    return classes.pop()


def route_output(port):
    """Send all of the process's output to the OutputBox at `port`."""
    config = get_config()
    for name in ('stdout', 'stderr'):
        if startup := OutputInterceptor.streams_connected[name]:
            startup.disconnect()
        OutputInterceptor('localhost', port, name, shared_secret=config['shared_secret'],
                          allow_insecure=config['allow_insecure']).connect()


def construct(cls, controls, window, routine_path, output_port):
    # Allocated apart from __init__(), so that the worker can install state
    # on the instance first.
    routine = cls.__new__(cls)
    routine._saved_controls = controls
    routine._saved_widgets = {}
    routine._figures = {}
    routine._folder = Path(routine_path)
    routine.window = window
    routine.output_port = output_port
    routine.values = namedtuple('Values', [])()
    routine.__init__()
    return routine


def save_layout(window):
    return {'geometry': bytes(window.saveGeometry()), 'state': bytes(window.saveState())}


def restore_layout(window, layout):
    """Restore the layout `save_layout` returned, and show the Output dock whatever it says."""
    for name, restore in [('geometry', window.restoreGeometry), ('state', window.restoreState)]:
        if layout.get(name) and not restore(layout[name]):
            print(f'Ignoring layout entry {name!r}: Qt rejects it.', file=sys.stderr)
    window.dock_output.show()


def read_saved_widgets(routine):
    """Return ``(snapshot, controls)``: the new `Routine.values` and the dict to save, read from
    the saved widgets on the GUI thread. A deleted widget is left out and unregistered."""
    controls = {}
    for name, widget in list(routine._saved_widgets.items()):
        try:
            controls[name] = widget.metaObject().userProperty().read(widget)
        except RuntimeError:
            del routine._saved_widgets[name]
    return namedtuple('Values', list(controls))(**controls), controls


class RoutineSettings:
    """The settings file of one routine: its control values and window layout.

    The file is named for the routine folder's full path, so routines whose folders
    share a name keep their own. Problems are reported on stderr, never raised."""

    def __init__(self, routine_path, config_dir):
        routine = Path(routine_path).resolve()
        digest = hashlib.sha256(str(routine).encode()).hexdigest()[:12]
        self.path = Path(config_dir) / f'lyse-routine-{routine.stem}-{digest}.toml'
        # An unreadable file that could not be renamed aside must not be saved over.
        self.writable = True

    def _report(self, message):
        print(f'{self.path}: {message}', file=sys.stderr)

    def load(self):
        """Return ``(controls, layout)``, the valid saved entries; layout values are bytes."""
        try:
            tables = load_appconfig(self.path)
        except (OSError, ValueError) as error:
            try:
                backups = (self.path.with_name(f'{self.path.name}.bad{n}') for n in count(1))
                backup = next(path for path in backups if not path.exists())
                self.path.rename(backup)
                outcome = f'it was renamed to {backup.name}'
            except OSError as rename_error:
                self.writable = False
                outcome = f'renaming it failed ({rename_error}), so no settings will be saved'
            self._report(f'cannot read the settings ({error}); {outcome}.')
            return {}, {}
        controls, layout = {}, {}
        for name, value in tables.get('controls', {}).items():
            if isinstance(value, CONTROL_VALUE_TYPES):
                controls[name] = value
            else:
                self._report(f'ignoring control {name!r}: it is not a number, string or boolean.')
        for name, text in tables.get('layout', {}).items():
            try:
                layout[name] = base64.b64decode(text, validate=True)
            except (TypeError, ValueError):
                self._report(f'ignoring layout entry {name!r}: it is not base64 text.')
        return controls, layout

    def save(self, controls, layout):
        """Write the controls and the layout, a dict whose values are bytes."""
        if not self.writable:
            return
        try:
            encoded = {name: base64.b64encode(data).decode() for name, data in layout.items()}
            save_appconfig(self.path, {'controls': controls, 'layout': encoded})
        except Exception as error:
            self._report(f'cannot save the settings ({error}).')
