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
import ast
import base64
import hashlib
import sys
from collections import namedtuple
from itertools import count
from keyword import iskeyword
from pathlib import Path

from labscript_utils.labconfig import load_appconfig, save_appconfig

CONTROL_VALUE_TYPES = (bool, int, float, str)


class Routine:
    """The base class of a lyse GUI routine.

    A routine file that declares ``LYSE_MODE = "gui"`` defines exactly one
    subclass. The worker constructs it once, calls `run` for each analysis,
    and calls `close` once as it quits.

    ``__init__()`` and `close` are optional. The worker constructs the object,
    so ``__init__()`` takes no arguments and need not call
    ``super().__init__()``.

    Attributes
    ----------
    values : namedtuple
        The saved controls' values by objectName; immutable, new for each analysis, empty at first.
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


def routine_mode(source, filename):
    """Return 'gui', or None for a classic script. Raise SyntaxError or
    ValueError if the file's mode cannot be read."""
    tree = ast.parse(source, filename)
    bindings = [node for node in ast.walk(tree)
                if (isinstance(node, ast.Name) and node.id == 'LYSE_MODE'
                    and isinstance(node.ctx, ast.Store))
                or (isinstance(node, ast.alias) and (node.asname or node.name) == 'LYSE_MODE')]
    if not bindings:
        return None
    declaration = ast.dump(ast.parse('LYSE_MODE = "gui"').body[0])
    if len(bindings) == 1 and declaration in map(ast.dump, tree.body):
        return 'gui'
    raise ValueError(
        f'{filename}: LYSE_MODE must be assigned once, at the top level, as "gui", or not at all.')


def routine_class(namespace):
    """Return the one Routine subclass that the executed file defines. Raise
    ValueError if it defines none or several."""
    # Classes defined in the file carry its __name__ as __module__; imported classes do not.
    classes = {value for value in namespace.values()
               if isinstance(value, type) and issubclass(value, Routine)
               and value.__module__ == namespace['__name__']}
    if not classes:
        raise ValueError('The routine file defines no subclass of lyse.Routine.')
    if len(classes) > 1:
        names = ', '.join(sorted(cls.__name__ for cls in classes))
        raise ValueError(
            f'The routine file defines more than one subclass of lyse.Routine: {names}. '
            'A shared base class belongs in an imported module.')
    return classes.pop()


def construct(cls, controls):
    # Allocated apart from __init__(), so that the worker can install state
    # on the instance first.
    routine = cls.__new__(cls)
    routine._saved_controls = controls
    routine._saved_widgets = {}
    routine.values = namedtuple('Values', [])()
    routine.__init__()
    return routine


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

    The file is named for the routine's full path, so routines with one file name
    in different folders keep their own. Problems are reported on stderr, never raised."""

    def __init__(self, routine_path, folder):
        routine = Path(routine_path).resolve()
        digest = hashlib.sha256(str(routine).encode()).hexdigest()[:12]
        self.path = Path(folder) / f'lyse-routine-{routine.stem}-{digest}.toml'
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
