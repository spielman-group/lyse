"""Painting the FileBox status column.

The progress-bar branch of ItemDelegate.paint runs for every shot in the
FileBox that has not finished analysing -- new_row starts them all at 0 percent
-- so it is on lyse's ordinary path, not an edge case. It is also the worst
place in the suite to raise: PyQt hands an exception in a virtual method to
sys.excepthook and then keeps painting, so a failure here does not stop lyse,
it repeats for every repaint. With labscript_utils.excepthook installed that is
an error window per repaint, and since the excepthook only limits how many are
open at once, closing one makes room for the next.
"""
import sys
import unittest

from qtutils.qt import QtCore, QtGui, QtWidgets

import lyse.widgets


def a_qapplication():
    """One QApplication for the whole process, as Qt requires."""
    return QtWidgets.QApplication.instance() or QtWidgets.QApplication(['test'])


class AnApp:
    """Stands in for the lyse app: the delegate only wants its QApplication."""

    def __init__(self, qapplication):
        self.qapplication = qapplication


class StatusColumnPaintTests(unittest.TestCase):
    COL_STATUS = 0
    ROLE_STATUS_PERCENT = QtCore.Qt.UserRole + 1

    def setUp(self):
        self.qapplication = a_qapplication()
        self.model = QtGui.QStandardItemModel(1, 2)
        self.view = QtWidgets.QTableView()
        self.view.setModel(self.model)
        self.delegate = lyse.widgets.ItemDelegate(
            AnApp(self.qapplication),
            self.view,
            self.model,
            self.COL_STATUS,
            self.ROLE_STATUS_PERCENT,
        )
        self.view.setItemDelegate(self.delegate)
        self.view.resize(200, 80)

    def set_percent(self, percent):
        item = self.model.item(0, self.COL_STATUS) or QtGui.QStandardItem()
        self.model.setItem(0, self.COL_STATUS, item)
        item.setData(percent, self.ROLE_STATUS_PERCENT)

    def paint_the_status_cell(self):
        """Call paint as the view does, but directly, so failures reach us.

        Through the view, PyQt would divert the exception to sys.excepthook and
        the test would pass while lyse filled the screen with error windows.
        """
        index = self.model.index(0, self.COL_STATUS)
        option = QtWidgets.QStyleOptionViewItem()
        option.rect = QtCore.QRect(0, 0, 70, 20)
        pixmap = QtGui.QPixmap(200, 80)
        painter = QtGui.QPainter(pixmap)
        try:
            self.delegate.paint(painter, option, index)
        finally:
            painter.end()

    def test_a_shot_still_being_analysed_paints(self):
        # Anything but 100 takes the progress-bar branch.
        self.set_percent(0)
        self.paint_the_status_cell()

    def test_a_partly_analysed_shot_paints(self):
        self.set_percent(42)
        self.paint_the_status_cell()

    def test_a_finished_shot_paints(self):
        # 100 renders as an ordinary item, showing the tick icon instead.
        self.set_percent(100)
        self.paint_the_status_cell()

    def test_painting_through_the_view_reaches_no_excepthook(self):
        # What the operator actually saw: an exception PyQt swallows into
        # sys.excepthook, once per repaint, forever.
        self.set_percent(0)
        seen = []
        original = sys.excepthook
        sys.excepthook = lambda cls, exc, tb: seen.append((cls, exc))
        try:
            self.view.show()
            for _ in range(3):
                self.view.viewport().repaint()
                self.qapplication.processEvents()
        finally:
            sys.excepthook = original
            self.view.hide()
        self.assertEqual(
            seen, [], 'painting the status column raised: %s' % (seen[:1],)
        )


if __name__ == '__main__':
    unittest.main()
