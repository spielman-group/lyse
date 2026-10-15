#####################################################################
#                                                                   #
# /utils/gui.py                                                     #
#                                                                   #
# Copyright 2013, Monash University                                 #
#                                                                   #
# This file is part of the program lyse, in the labscript suite     #
# (see http://labscriptsuite.org), and is licensed under the        #
# Simplified BSD License. See the license.txt file in the root of   #
# the project for the full license.                                 #
#                                                                   #
#####################################################################
"""Lyse GUI common utilities
"""

import numpy as np


def scientific_notation(x, sigfigs=4, mode='eng'):
    """Return the float x as a unicode string in scientific notation.

    mode is 'eng', which uses an SI prefix where there is one, or 'exponential'."""

    times = '\u00d7'
    thinspace = '\u2009'
    hairspace = '\u200a'
    sups = {'-': '\u207b',
            '0': '\u2070',
            '1': '\xb9',
            '2': '\xb2',
            '3': '\xb3',
            '4': '\u2074',
            '5': '\u2075',
            '6': '\u2076',
            '7': '\u2077',
            '8': '\u2078',
            '9': '\u2079'}

    prefixes = {
        -24: "y",
        -21: "z",
        -18: "a",
        -15: "f",
        -12: "p",
        -9: "n",
        -6: "\u03bc",
        -3: "m",
        0: "",
        3: "k",
        6: "M",
        9: "G",
        12: "T",
        15: "P",
        18: "E",
        21: "Z",
        24: "Y"
    }

    if not isinstance(x, float):
        raise TypeError('x must be floating point number')
    if np.isnan(x) or np.isinf(x):
        return str(x)
    if x != 0:
        exponent = int(np.floor(np.log10(np.abs(x))))
        # Only multiples of 10^3
        exponent = int(np.floor(exponent / 3) * 3)
    else:
        exponent = 0

    significand = x / 10 ** exponent
    pre_decimal, post_decimal = divmod(significand, 1)
    digits = sigfigs - len(str(int(pre_decimal)))
    significand = round(significand, digits)
    result = str(significand)
    if exponent:
        if mode == 'exponential':
            superscript = ''.join(sups.get(char, char) for char in str(exponent))
            result += thinspace + times + thinspace + '10' + superscript
        elif mode == 'eng':
            try:
                # If our number has an SI prefix then use it
                prefix = prefixes[exponent]
                result += hairspace + prefix
            except KeyError:
                # Otherwise display in scientific notation
                superscript = ''.join(sups.get(char, char) for char in str(exponent))
                result += thinspace + times + thinspace + '10' + superscript
    return result
