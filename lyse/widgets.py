#####################################################################
#                                                                   #
# /widgets.py                                                      #
#                                                                   #
# Copyright 2013, Monash University                                 #
#                                                                   #
# This file is part of the program lyse, in the labscript suite     #
# (see http://labscriptsuite.org), and is licensed under the        #
# Simplified BSD License. See the license.txt file in the root of   #
# the project for the full license.                                 #
#                                                                   #
#####################################################################
"""Lyse GUI widgets
"""

# qt imports
from qtutils.qt import QtCore, QtGui, QtWidgets
from qtutils.qt.QtCore import pyqtSignal as Signal

class UneditableModel(QtGui.QStandardItemModel):

    def flags(self, index):
        """Return flags as normal except that the ItemIsEditable
        flag is always False"""
        result = QtGui.QStandardItemModel.flags(self, index)
        return result & ~QtCore.Qt.ItemIsEditable

class EditColumnsDialog(QtWidgets.QDialog):
    close_signal = Signal()

    def __init__(self):
        QtWidgets.QDialog.__init__(self, None, QtCore.Qt.WindowSystemMenuHint | QtCore.Qt.WindowTitleHint)

    def closeEvent(self, event):
        self.close_signal.emit()
        event.ignore()


class ItemDelegate(QtWidgets.QStyledItemDelegate):

    """An item delegate with a fixed height and a progress bar in one column"""
    EXTRA_ROW_HEIGHT = 2

    def __init__(self, app, view, model, col_status, role_status_percent):
        self.app = app
        self.view = view
        self.model = model
        self.COL_STATUS = col_status
        self.ROLE_STATUS_PERCENT = role_status_percent
        QtWidgets.QStyledItemDelegate.__init__(self)

    def sizeHint(self, *args, **kwargs):
        fontmetrics = QtGui.QFontMetrics(self.view.font())
        text_height = fontmetrics.height()
        row_height = text_height + self.EXTRA_ROW_HEIGHT
        size = QtWidgets.QStyledItemDelegate.sizeHint(self, *args, **kwargs)
        return QtCore.QSize(size.width(), row_height)

    def paint(self, painter, option, index):
        if index.column() == self.COL_STATUS:
            status_percent = self.model.data(index, self.ROLE_STATUS_PERCENT)
            if status_percent == 100:
                # Render as a normal item - this shows whatever icon is set instead of a progress bar.
                return QtWidgets.QStyledItemDelegate.paint(self, painter, option, index)
            else:
                # Method of rendering a progress bar into the view copied from
                # Qt's 'network-torrent' example:
                # http://qt-project.org/doc/qt-4.8/network-torrent-torrentclient-cpp.html

                # Set up a QStyleOptionProgressBar to precisely mimic the
                # environment of a progress bar.
                progress_bar_option = QtWidgets.QStyleOptionProgressBar()
                progress_bar_option.state = QtWidgets.QStyle.State_Enabled
                progress_bar_option.direction = self.app.qapplication.layoutDirection()
                progress_bar_option.rect = option.rect
                # option.fontMetrics, not the application's: Qt6 removed
                # QApplication.fontMetrics(), and the metrics Qt already
                # computed for this item are the ones sizeHint above sizes the
                # row with.
                progress_bar_option.fontMetrics = option.fontMetrics
                progress_bar_option.minimum = 0
                progress_bar_option.maximum = 100
                progress_bar_option.textAlignment = QtCore.Qt.AlignCenter
                progress_bar_option.textVisible = True

                # Set the progress and text values of the style option.
                progress_bar_option.progress = int(status_percent)
                progress_bar_option.text = '%d%%' % status_percent

                # Draw the progress bar onto the view.
                self.app.qapplication.style().drawControl(QtWidgets.QStyle.CE_ProgressBar, progress_bar_option, painter)
        else:
            return QtWidgets.QStyledItemDelegate.paint(self, painter, option, index)
