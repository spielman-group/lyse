#####################################################################
#                                                                   #
# /plugins/__init__.py                                              #
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
"""Plugin interfaces and UI contexts. lyse discovers enabled plugins at application
startup, from folders here and installed ``lyse.plugins`` entry points."""
from qtutils.qt import QtGui
# Plugin modules import BasePlugin, callback, Callback and DEFAULT_PRIORITY
# from here.
from labscript_utils.plugins import (
    DEFAULT_PRIORITY,
    BasePlugin,
    Callback,
    callback,
)


class Areas:
    """The ``'areas'`` plugin context. A contribution
    ``{'context': 'areas', 'area': name, 'widget': widget}`` adds the widget to the
    end of the named area of lyse's window: ``'analysis_controls'``, beside Pause and
    Abort, or ``'shots_toolbar'``."""

    def __init__(self, layouts):
        self.layouts = layouts

    def add(self, plugin_name, contribution, data):
        self.layouts[contribution['area']].addWidget(contribution['widget'])


class Tabs:
    """Embed one panel per plugin beside Output. Contributions have ``widget``,
    ``label`` and an optional QIcon ``icon``; lyse owns selection and persistence.
    """

    def __init__(self, widget):
        self.widget = widget

    def add(self, plugin_name, contribution, data):
        panel = contribution['widget']
        panel.setProperty('lyse_plugin_name', plugin_name)
        self.widget.addTab(panel, contribution.get('icon', QtGui.QIcon()), contribution['label'])
