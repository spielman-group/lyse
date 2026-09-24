#####################################################################
#                                                                   #
# /tests/conftest.py                                                #
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
"""Keep windows and error dialogs off the screen while the tests run."""
import os

# In a conftest, not a fixture, because pytest imports it before any test
# module, and so before anything reads these; setdefault keeps a value you
# have exported yourself.
os.environ.setdefault('QT_QPA_PLATFORM', 'offscreen')
os.environ.setdefault('LABSCRIPT_NO_ERROR_DIALOG', '1')
