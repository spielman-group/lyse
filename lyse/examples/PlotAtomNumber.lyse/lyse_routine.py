#####################################################################
#                                                                   #
# /lyse/examples/PlotAtomNumber.lyse/lyse_routine.py                #
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
"""Plot the atom number of each shot in the most recent sequences.

A lyse GUI routine. In lyse's Multishot routines box, choose "Add a GUI routine
folder (.lyse)…" and select the folder PlotAtomNumber.lyse. Its run() reads the
atom numbers that ComputeAtomNumber.lyse saved from lyse's dataframe, which
self.data() returns, so add that folder to the Singleshot routines box too. The
window has a spin box choosing how many recent sequences to plot, which lyse
saves between sessions, and an Atom number dock.
"""

import lyse
from qtutils import inmain_decorator


class PlotAtomNumber(lyse.Routine):
    def __init__(self):
        self.ui = self.load_ui("PlotAtomNumber.ui")
        # lyse restores the saved count now, and hands run() its value as self.values.
        self.saved_widgets(self.ui.sequences)
        self.axes = self.add_figure("Atom number").subplots()

    def run(self):
        df = self.data(n_sequences=self.values.sequences)
        column = ("ComputeAtomNumber", "atom_number")
        if column in df:
            # run() is on lyse's analysis thread; plot() draws on the GUI thread.
            self.plot(df[column].values)
        else:
            # No shot has been analysed by ComputeAtomNumber.lyse yet.
            print("No atom numbers to plot yet.")

    @inmain_decorator()
    def plot(self, atom_number):
        self.axes.clear()
        self.axes.plot(atom_number, "o-")
        self.axes.set_xlabel("Shot #")
        self.axes.set_ylabel("Atoms")
        self.axes.figure.canvas.draw_idle()
