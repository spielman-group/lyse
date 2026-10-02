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
"""Reading the mode a routine file declares, without executing the file."""
import ast


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
