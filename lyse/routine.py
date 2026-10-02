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
"""The Routine base class of a GUI routine file, and the steps from the file to a
routine: reading its mode, finding its class and constructing it."""
import ast


class Routine:
    """The base class of a lyse GUI routine.

    A routine file that declares ``LYSE_MODE = "gui"`` defines exactly one
    subclass. The worker constructs it once, calls `run` for each analysis,
    and calls `close` once as it quits.

    ``__init__()`` and `close` are optional. The worker constructs the object,
    so ``__init__()`` takes no arguments and need not call
    ``super().__init__()``.
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


def construct(cls):
    # Allocated apart from __init__(), so that the worker can install state
    # on the instance first.
    routine = cls.__new__(cls)
    routine.__init__()
    return routine
