#####################################################################
#                                                                   #
# /lyse/examples/plot_atom_number.py                                #
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
"""Plot the atom number of each shot in the most recent sequence.

A classic lyse script. In lyse's Multishot routines box, choose "Add classic
scripts (.py)…" and select this file. It reads the atom numbers that
``compute_atom_number.py`` saved from lyse's dataframe, which ``lyse.data()``
returns, so add that script to the Singleshot routines box too.
"""

import lyse
from matplotlib import pyplot as plt

df = lyse.data(n_sequences=1)
column = ("compute_atom_number", "atom_number")

fig = plt.figure("Atom number")
ax = fig.add_subplot(1, 1, 1)
if column in df:
    ax.plot(df[column].values, "o-")
else:
    # No shot of the sequence has been analysed by compute_atom_number.py yet.
    print("No atom numbers to plot yet.")
ax.set_xlabel("Shot #")
ax.set_ylabel("Atoms")
