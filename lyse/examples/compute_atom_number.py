#####################################################################
#                                                                   #
# /lyse/examples/compute_atom_number.py                             #
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

A classic lyse script. In lyse's Singleshot routines box, choose "Add classic
scripts (.py)…" and select this file. lyse runs it once for each shot, with
``lyse.path`` naming the shot file. It saves ``integrated_od`` and
``atom_number`` in the results group ``compute_atom_number``, which lyse names
after this file, and draws each shot's images and optical density.
``plot_atom_number.py`` plots the atom numbers it saves.
"""

import lyse
import numpy as np
from matplotlib import pyplot as plt

# The resonant absorption cross section of Rb-87 on the D2 line, in microns^2.
CROSS_SECTION = 0.2907

# The camera's pixel pitch, in microns, and its magnification. A pixel's side
# in the object plane is pixel_size / magnification.
PIXEL_SIZE = 5.2
MAGNIFICATION = 2.0
PIXEL_AREA = (PIXEL_SIZE / MAGNIFICATION) ** 2

run = lyse.Run(lyse.path)

# The absorption images: with atoms, with probe light alone, and dark.
images = run.get_images_dict("side", "absorption", "atoms", "dark", "probe")

# Subtract as floats: unsigned integer counts would wrap round to a huge value
# wherever a pixel is dimmer than the dark frame.
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

# lyse shows the figures a script makes once it ends, so no show call is needed.
fig = plt.figure("OD", layout="constrained")
panels = {
    "Atoms": images["atoms"],
    "Probe": images["probe"],
    "Dark": images["dark"],
    "OD": od,
}
for index, (title, image) in enumerate(panels.items(), start=1):
    ax = fig.add_subplot(2, 2, index)
    ax.set_title(title, fontsize=8)
    ax.imshow(image)
