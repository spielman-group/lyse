"""The FileBox status column, painted for a shot still being analysed.

A failure while painting does not reach the caller: PyQt hands it to
sys.excepthook and paints again, so with lyse's excepthook installed it is an
error window on every repaint.
"""
import sys
import types
import unittest

from qtutils.qt import QtCore, QtGui, QtWidgets

import lyse.widgets


class StatusColumnTests(unittest.TestCase):

    def test_a_shot_being_analysed_paints_without_error(self):
        qapplication = QtWidgets.QApplication.instance() or QtWidgets.QApplication(['test'])
        status, role = 0, QtCore.Qt.UserRole + 1
        model = QtGui.QStandardItemModel(1, 2)
        item = QtGui.QStandardItem()
        item.setData(42, role)
        model.setItem(0, status, item)
        view = QtWidgets.QTableView()
        view.setModel(model)
        app = types.SimpleNamespace(qapplication=qapplication)
        view.setItemDelegate(lyse.widgets.ItemDelegate(app, view, model, status, role))
        seen = []
        original = sys.excepthook
        sys.excepthook = lambda cls, exc, tb: seen.append(exc)
        try:
            view.show()
            view.viewport().repaint()
            qapplication.processEvents()
        finally:
            sys.excepthook = original
            view.hide()
        self.assertEqual(seen, [])


if __name__ == '__main__':
    unittest.main()
