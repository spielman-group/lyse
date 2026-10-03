#####################################################################
#                                                                   #
# /lyse/examples/ComputeAtomNumber.lyse/lyse_routine.py             #
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
"""Compute the optical density and atom number of an absorption image.

A lyse GUI routine. In lyse's Singleshot routines box, choose "Add a GUI routine
folder (.lyse)…" and select the folder ComputeAtomNumber.lyse. lyse builds the
class below once, and calls its run() for each shot, with self.path naming the
shot file. run() saves integrated_od and atom_number in the results group
ComputeAtomNumber, which lyse names after the class. The window has two spin
boxes for a guess of the cloud's center, which lyse saves between sessions, and
an OD dock showing each shot's images with lines at the guess.
PlotAtomNumber.lyse plots the atom numbers it saves.
"""

import lyse
import numpy as np
from qtutils import inmain_decorator

# The resonant absorption cross section of Rb-87 on the D2 line, in microns^2.
CROSS_SECTION = 0.2907

# The camera's pixel pitch, in microns, and its magnification. A pixel's side
# in the object plane is pixel_size / magnification.
PIXEL_SIZE = 5.2
MAGNIFICATION = 2.0
PIXEL_AREA = (PIXEL_SIZE / MAGNIFICATION) ** 2


class ComputeAtomNumber(lyse.Routine):
    def __init__(self):
        self.ui = self.load_ui("ComputeAtomNumber.ui")
        # lyse restores the saved guess now, and saves it before each analysis.
        self.saved_widgets(self.ui.x_center, self.ui.y_center)
        self.figure = self.add_figure("OD")
        self.figure.set_layout_engine("constrained")
        # The (vertical, horizontal) pair of lines plot() draws on each axes.
        self.lines = []
        self.ui.x_center.valueChanged.connect(self.move_lines)
        self.ui.y_center.valueChanged.connect(self.move_lines)

    def run(self):
        run = self.get_run()

        # The absorption images: with atoms, with probe light alone, and dark.
        images = run.get_images_dict("side", "absorption", "atoms", "dark", "probe")

        # Subtract as floats: unsigned integer counts would wrap round to a huge
        # value wherever a pixel is dimmer than the dark frame.
        dark = images["dark"].astype(float)
        atoms = images["atoms"] - dark
        probe = images["probe"] - dark

        with np.errstate(divide="ignore", invalid="ignore"):
            od = -np.log(atoms / probe)
        # Replace +/- infinity with NaN so np.nansum excludes them.
        od[~np.isfinite(od)] = np.nan

        # Summed optical density is in pixels; an atom number is not. Each pixel's
        # optical density stands for the column density over that pixel's area, and a
        # column density becomes a number of atoms through the cross section.
        #
        # A pixel the cloud absorbed entirely has no optical density to read -- the
        # logarithm of zero is not finite, and nansum counts it as nothing rather than
        # as the large value it stands for. So this undercounts a cloud dense enough to
        # extinguish the probe, and the undercount grows with density: past that point
        # a larger cloud can measure smaller.
        integrated_od = np.nansum(od)
        atom_number = integrated_od * PIXEL_AREA / CROSS_SECTION

        run.save_result("integrated_od", integrated_od)
        run.save_result("atom_number", atom_number)

        # run() is on lyse's analysis thread; plot() draws on the GUI thread.
        self.plot(images, od)

    @inmain_decorator()
    def plot(self, images, od):
        # The image sets the spin boxes' range, and a guess outside it, as an
        # unset 0 is, moves to its center.
        height, width = od.shape
        for spin_box, size in (self.ui.x_center, width), (self.ui.y_center, height):
            spin_box.setMaximum(size)
            if not 0 < spin_box.value() < size:
                spin_box.setValue(size / 2)

        # Read from the spin boxes, not self.values, so that the lines match them
        # even if one moved during the analysis.
        x, y = self.ui.x_center.value(), self.ui.y_center.value()
        panels = {
            "Atoms": images["atoms"],
            "Probe": images["probe"],
            "Dark": images["dark"],
            "OD": od,
        }
        self.figure.clear()
        self.lines = []
        for index, (title, image) in enumerate(panels.items(), start=1):
            ax = self.figure.add_subplot(2, 2, index)
            ax.set_title(title, fontsize=8)
            ax.imshow(image)
            self.lines.append((ax.axvline(x), ax.axhline(y)))
        self.figure.canvas.draw_idle()

    def move_lines(self):
        # A Qt slot, so on the GUI thread. It moves the lines at once, rather than
        # at the next shot; before the first shot there are none.
        x, y = self.ui.x_center.value(), self.ui.y_center.value()
        for vertical, horizontal in self.lines:
            vertical.set_xdata([x, x])
            horizontal.set_ydata([y, y])
        if self.lines:
            self.figure.canvas.draw_idle()
