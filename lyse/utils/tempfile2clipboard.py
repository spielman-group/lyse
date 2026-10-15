#####################################################################
#                                                                   #
# /utils/tempfile2clipboard.py                                      #
#                                                                   #
# Copyright 2017, Monash University                                 #
#                                                                   #
# This file is part of the program lyse, in the labscript suite     #
# (see http://labscriptsuite.org), and is licensed under the        #
# Simplified BSD License. See the license.txt file in the root of   #
# the project for the full license.                                 #
#                                                                   #
#####################################################################
"""Copy an image to the clipboard, and keep running until the clipboard changes.

usage:

    python tempfile2clipboard.py [--delete] image_filepath

With --delete, the image file is deleted once it has been read. The script keeps
running because some clipboards request the data from the application only when
it is pasted. It is a stand-alone script because it needs a Qt mainloop, which
would otherwise interfere with any use of Qt in the calling program.
"""

import sys
import os

from qtutils.qt.QtWidgets import QApplication
from qtutils.qt.QtGui import QImage

def main():
    USAGE = 'Usage:\n    python tempfile2clipboard.py [--delete] image_filepath\n'

    if len(sys.argv) > 1 and sys.argv[1] == '--delete':
        delete = True
        del sys.argv[1]
    else:
        delete = False

    if len(sys.argv) != 2:
        sys.stderr.write("Invalid arguments.\n" + USAGE)
        sys.exit(1)

    image_file = sys.argv[1]
    image = QImage(image_file)
    if delete:
        os.unlink(image_file)

    if image.isNull():
        sys.stderr.write(f"Invalid image file: {image_file}.\n" + USAGE)
        sys.exit(1)

    app = QApplication([])
    app.clipboard().setImage(image)

    # Keep running until the clipboard contents change to something else:
    app.clipboard().dataChanged.connect(app.quit)
    app.exec()


if __name__ == '__main__':
    main()
