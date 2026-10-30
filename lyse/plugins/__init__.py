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
"""lyse's plugins: each folder here, and each installed package registered under
the ``lyse.plugins`` entry point, loaded if its flag in the labconfig's
``[lyse/plugins]`` section is true."""
import logging
import os

from labscript_utils.labconfig import LabConfig
# Plugin modules import BasePlugin, callback, Callback and DEFAULT_PRIORITY
# from here.
from labscript_utils.plugins import (
    DEFAULT_PRIORITY,
    BasePlugin,
    Callback,
    PluginManager,
    callback,
)
from lyse.utils import LYSE_DIR

PLUGINS_DIR = os.path.join(LYSE_DIR, 'plugins')

logger = logging.getLogger('lyse.plugins')

PLUGIN_CONFIG_SECTION = 'lyse/plugins'

manager = PluginManager(
    'lyse.plugins',
    PLUGINS_DIR,
    LabConfig(),
    PLUGIN_CONFIG_SECTION,
    (),
    logger,
    entry_point_group='lyse.plugins',
)
modules = manager.discover_modules()


class Areas:
    """The ``'areas'`` plugin context. A contribution
    ``{'context': 'areas', 'area': name, 'widget': widget}`` adds the widget to the
    end of the named area of lyse's window: ``'analysis_controls'``, beside Pause and
    Abort, or ``'shots_toolbar'``."""

    def __init__(self, layouts):
        self.layouts = layouts

    def add(self, plugin_name, contribution, data):
        self.layouts[contribution['area']].addWidget(contribution['widget'])
