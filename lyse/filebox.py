#####################################################################
#                                                                   #
# /filebox.py                                                       #
#                                                                   #
# Copyright 2013, Monash University                                 #
#                                                                   #
# This file is part of the program lyse, in the labscript suite     #
# (see http://labscriptsuite.org), and is licensed under the        #
# Simplified BSD License. See the license.txt file in the root of   #
# the project for the full license.                                 #
#                                                                   #
#####################################################################
"""Lyse GUI dataframe filebox widget
"""

import itertools
import os
import re
import sys
import logging
import threading
import time
import traceback
import queue

# 3rd party imports:
import numpy as np
import labscript_utils.h5_lock, h5py # use patched h5py
from labscript_utils.qtwidgets.application import monospace_font
import pandas

# Labscript imports
import zprocess
from labscript_utils.labconfig import LabConfig
from labscript_utils.programs import open_file
from labscript_utils.qtwidgets.headerview_with_widgets import HorizontalHeaderViewWithWidgets
from labscript_utils.qtwidgets.leftclick import TableView
import labscript_utils.qtwidgets.file_dialogs as file_dialogs


# qt imports
from qtutils.qt import QtCore, QtGui, QtWidgets
from qtutils.qt.QtCore import pyqtSignal as Signal
from qtutils import inmain_decorator, UiLoader, DisconnectContextManager
from qtutils.auto_scroll_to_end import set_auto_scroll_to_end
import qtutils.icons
from qtutils.icons import Icon

# Lyse imports
from lyse.dataframe_utilities import (concat_with_padding, get_dataframe_from_shot,
                                      flatten_dict, pad_columns)
import lyse.plugins
import lyse.utils
import lyse.utils.gui
import lyse.widgets


def natural_sort_key(path):
    """Sort key for a string that compares runs of digits by their integer value."""
    parts = re.split(r'(\d+)', path)
    parts[1::2] = map(int, parts[1::2])
    return parts



# The tooltip of a shot's status cell, by the state of its analysis:
STATE_TOOLTIPS = {
    'awaiting multishot': 'Its single-shot routines have run, and it awaits the multishot routines',
    'failed': 'A routine did not end done; the others ran',
    'stopped': 'Its routines stopped at the first that failed',
    'aborted': 'Its analysis was aborted',
    'deleted': 'Shot has been deleted off disk or is unreadable',
}


def outcome_state(outcomes):
    """The state in which an attempt with these outcomes of its routines ends."""
    outcomes = set(outcomes.values())
    if 'aborted' in outcomes:
        return 'aborted'
    if 'not run' in outcomes:
        return 'stopped'
    return 'done' if outcomes <= {'done'} else 'failed'


class EditColumns:
    ROLE_SORT_DATA = QtCore.Qt.UserRole + 1
    COL_VISIBLE = 0
    COL_NAME = 1

    def __init__(self, filebox, column_names, columns_visible):
        self.filebox = filebox
        self.column_names = column_names.copy()
        self.columns_visible = columns_visible.copy()
        self.old_columns_visible = columns_visible.copy()

        loader = UiLoader()
        self.ui = loader.load(os.path.join(lyse.utils.LYSE_DIR, 'user_interface/edit_columns.ui'), lyse.widgets.EditColumnsDialog())

        self.model = QtGui.QStandardItemModel()
        self.header = HorizontalHeaderViewWithWidgets(self.model)
        self.select_all_checkbox = QtWidgets.QCheckBox()
        self.select_all_checkbox.setTristate(False)
        self.ui.treeView.setHeader(self.header)
        self.proxy_model = QtCore.QSortFilterProxyModel()
        self.proxy_model.setSourceModel(self.model)
        self.proxy_model.setFilterCaseSensitivity(QtCore.Qt.CaseInsensitive)
        self.proxy_model.setFilterKeyColumn(self.COL_NAME)
        self.ui.treeView.setSortingEnabled(True)
        self.header.setStretchLastSection(True)
        self.proxy_model.setSortRole(self.ROLE_SORT_DATA)
        self.ui.treeView.setModel(self.proxy_model)
        # Set once: clearing the model would delete the header items the header view holds.
        self.model.setHorizontalHeaderLabels(['', 'Name'])
        self.header.setWidget(self.COL_VISIBLE, self.select_all_checkbox)
        self.ui.treeView.resizeColumnToContents(self.COL_VISIBLE)
        self.ui.treeView.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        self.ui.setWindowModality(QtCore.Qt.ApplicationModal)

        self.ui.treeView.setContextMenuPolicy(QtCore.Qt.CustomContextMenu)
        self.action_set_selected_visible = QtWidgets.QAction(
            Icon(':qtutils/fugue/ui-check-box'), 'Show selected columns',  self.ui)
        self.action_set_selected_hidden = QtWidgets.QAction(
            Icon(':qtutils/fugue/ui-check-box-uncheck'), 'Hide selected columns',  self.ui)

        self.connect_signals()
        self.populate_model(column_names, self.columns_visible)

    def connect_signals(self):
        self.ui.close_signal.connect(self.close)
        self.ui.lineEdit_filter.textEdited.connect(self.on_filter_text_edited)
        self.ui.pushButton_make_it_so.clicked.connect(self.make_it_so)
        self.ui.pushButton_cancel.clicked.connect(self.cancel)
        self.model.itemChanged.connect(self.on_model_item_changed)
        self.model_item_changed_disconnected = DisconnectContextManager(
            self.model.itemChanged, self.on_model_item_changed)
        self.select_all_checkbox.stateChanged.connect(self.on_select_all_state_changed)
        self.select_all_checkbox_state_changed_disconnected = DisconnectContextManager(
            self.select_all_checkbox.stateChanged, self.on_select_all_state_changed)
        self.ui.treeView.customContextMenuRequested.connect(self.on_treeView_context_menu_requested)
        self.action_set_selected_visible.triggered.connect(
            lambda: self.on_set_selected_triggered(QtCore.Qt.Checked))
        self.action_set_selected_hidden.triggered.connect(
            lambda: self.on_set_selected_triggered(QtCore.Qt.Unchecked))

    def populate_model(self, column_names, columns_visible):
        self.model.removeRows(0, self.model.rowCount())
        # Which indices in self.columns_visible the row numbers correspond to
        self.column_indices = {}

        # Remove our special columns from the dict of column names by keeping only tuples:
        column_names = {i: name for i, name in column_names.items() if isinstance(name, tuple)}

        sortkey = lambda item: ', '.join(item[1]).lower().strip(', ')

        for column_index, name in sorted(column_names.items(), key=sortkey):
            visible = columns_visible[column_index]
            visible_item = QtGui.QStandardItem()
            visible_item.setCheckable(True)
            if visible:
                visible_item.setCheckState(QtCore.Qt.Checked)
                visible_item.setData(QtCore.Qt.Checked, self.ROLE_SORT_DATA)
            else:
                visible_item.setCheckState(QtCore.Qt.Unchecked)
                visible_item.setData(QtCore.Qt.Unchecked, self.ROLE_SORT_DATA)
            name_as_string = ', '.join(name).strip(', ')
            name_item = QtGui.QStandardItem(name_as_string)
            name_item.setData(sortkey((column_index, name)), self.ROLE_SORT_DATA)
            self.model.appendRow([visible_item, name_item])
            self.column_indices[self.model.rowCount() - 1] = column_index

        self.ui.treeView.resizeColumnToContents(self.COL_NAME)
        self.update_select_all_checkstate()
        self.ui.treeView.sortByColumn(self.COL_NAME, QtCore.Qt.AscendingOrder)

    def on_treeView_context_menu_requested(self, point):
        menu = QtWidgets.QMenu(self.ui)
        menu.addAction(self.action_set_selected_visible)
        menu.addAction(self.action_set_selected_hidden)
        menu.exec(QtGui.QCursor.pos())

    def on_set_selected_triggered(self, visible):
        selected_indexes = self.ui.treeView.selectedIndexes()
        selected_rows = set(self.proxy_model.mapToSource(index).row() for index in selected_indexes)
        for row in selected_rows:
            visible_item = self.model.item(row, self.COL_VISIBLE)
            self.update_visible_state(visible_item, visible)
        self.update_select_all_checkstate()
        self.do_sort()
        self.filebox.set_columns_visible(self.columns_visible)

    def on_filter_text_edited(self, text):
        self.proxy_model.setFilterWildcard(text)

    def on_select_all_state_changed(self, state):
        with self.select_all_checkbox_state_changed_disconnected:
            # Do not allow a switch *to* a partially checked state:
            self.select_all_checkbox.setTristate(False)
        state = self.select_all_checkbox.checkState()
        for row in range(self.model.rowCount()):
            visible_item = self.model.item(row, self.COL_VISIBLE)
            self.update_visible_state(visible_item, state)
        self.do_sort()

        self.filebox.set_columns_visible(self.columns_visible)

    def update_visible_state(self, item, state):
        assert item.column() == self.COL_VISIBLE, "unexpected column"
        row = item.row()
        with self.model_item_changed_disconnected:
            item.setCheckState(state)
            item.setData(state, self.ROLE_SORT_DATA)
            if state == QtCore.Qt.Checked:
                self.columns_visible[self.column_indices[row]] = True
            else:
                self.columns_visible[self.column_indices[row]] = False

    def update_select_all_checkstate(self):
        with self.select_all_checkbox_state_changed_disconnected:
            all_states = []
            for row in range(self.model.rowCount()):
                visible_item = self.model.item(row, self.COL_VISIBLE)
                all_states.append(visible_item.checkState())
            if all(state == QtCore.Qt.Checked for state in all_states):
                self.select_all_checkbox.setCheckState(QtCore.Qt.Checked)
            elif all(state == QtCore.Qt.Unchecked for state in all_states):
                self.select_all_checkbox.setCheckState(QtCore.Qt.Unchecked)
            else:
                self.select_all_checkbox.setCheckState(QtCore.Qt.PartiallyChecked)

    def on_model_item_changed(self, item):
        state = item.checkState()
        self.update_visible_state(item, state)
        self.update_select_all_checkstate()
        self.do_sort()
        self.filebox.set_columns_visible(self.columns_visible)

    def do_sort(self):
        header = self.ui.treeView.header()
        sort_column = header.sortIndicatorSection()
        sort_order = header.sortIndicatorOrder()
        self.ui.treeView.sortByColumn(sort_column, sort_order)

    def update_columns(self, column_names, columns_visible):

        # Index/name mapping may have changed. Get a mapping by *name* of
        # which columns were previously visible, so we can update our by-index
        # mapping in a moment:
        old_columns_visible_by_name = {}
        for old_column_number, visible in self.old_columns_visible.items():
            column_name = self.column_names[old_column_number]
            old_columns_visible_by_name[column_name] = visible

        self.columns_visible = columns_visible.copy()
        self.column_names = column_names.copy()

        self.old_columns_visible = {}
        for index, name in self.column_names.items():
            try:
                self.old_columns_visible[index] = old_columns_visible_by_name[name]
            except KeyError:
                # A new column. If editing is cancelled, any new columns
                # should be set to visible:
                self.old_columns_visible[index] = True
        self.populate_model(column_names, self.columns_visible)

    def show(self):
        self.old_columns_visible = self.columns_visible.copy()
        self.ui.show()

    def close(self):
        self.columns_visible = self.old_columns_visible.copy()
        self.filebox.set_columns_visible(self.columns_visible)
        self.populate_model(self.column_names, self.columns_visible)
        self.ui.hide()

    def cancel(self):
        self.ui.close()

    def make_it_so(self):
        self.ui.hide()


def format_value(value):
    """Return the text of a dataframe value for the shots table: the first line of its
    string, in scientific notation if it is a float"""
    if isinstance(value, float):
        value_str = lyse.utils.gui.scientific_notation(value)
    else:
        value_str = str(value)
    lines = value_str.splitlines()
    if len(lines) > 1:
        return lines[0] + ' ...'
    return value_str


def role_number(role):
    """Return the number of a Qt.ItemDataRole, which is the type of the role argument
    of QAbstractItemModel.data()"""
    return int(getattr(role, 'value', role))


class ShotsTableModel(QtCore.QAbstractTableModel):
    """The Qt model of the shots table of a DataFrameModel. There is one row per shot,
    and the columns are the analysis status followed by the dataframe columns, numbered
    as in DataFrameModel.column_names. Values are read from the dataframe and formatted
    when the view requests them."""

    DISPLAY_ROLE = role_number(QtCore.Qt.DisplayRole)
    DECORATION_ROLE = role_number(QtCore.Qt.DecorationRole)
    TOOLTIP_ROLE = role_number(QtCore.Qt.ToolTipRole)
    TEXT_ALIGNMENT_ROLE = role_number(QtCore.Qt.TextAlignmentRole)

    def __init__(self, shots):
        QtCore.QAbstractTableModel.__init__(self)
        self.shots = shots
        # Positions of the columns in the dataframe, by column number:
        self._positions = {}
        self._status_icon = Icon(':qtutils/fugue/information')
        self._state_icons = {
            'done': Icon(':qtutils/fugue/tick'),
            'awaiting multishot': Icon(':qtutils/fugue/hourglass'),
            'failed': Icon(':qtutils/fugue/exclamation'),
            'stopped': Icon(':qtutils/fugue/exclamation'),
            'aborted': Icon(':qtutils/fugue/cross-octagon'),
            'deleted': Icon(':qtutils/fugue/drive--minus'),
        }
        self._alignment = QtCore.Qt.AlignCenter

    def dataframe_changed(self):
        """Must be called when the dataframe is replaced or its columns are renumbered"""
        self._positions = {}

    def value(self, row, column):
        """Return the dataframe value of a cell of the table, as a Python scalar if it is
        a NumPy scalar"""
        try:
            position = self._positions[column]
        except KeyError:
            column_name = self.shots.column_names[column]
            position = self.shots.dataframe.columns.get_loc(column_name)
            self._positions[column] = position
        value = self.shots.dataframe.iat[row, position]
        if isinstance(value, np.generic):
            return value.item()
        return value

    def rowCount(self, parent=QtCore.QModelIndex()):
        return 0 if parent.isValid() else len(self.shots.filepaths)

    def columnCount(self, parent=QtCore.QModelIndex()):
        return 0 if parent.isValid() else len(self.shots.column_names)

    def data(self, index, role=DISPLAY_ROLE):
        shots = self.shots
        row = index.row()
        if index.column() == shots.COL_STATUS:
            state = shots.states[row]
            if role == shots.ROLE_STATUS_PERCENT:
                # A progress bar only while the shot is not done:
                return shots.status_percent[row] if state == 'not done' else 100
            if role == shots.ROLE_STATE:
                return state
            if role == self.DECORATION_ROLE:
                return self._state_icons.get(state)
            if role == self.TOOLTIP_ROLE:
                return STATE_TOOLTIPS.get(state)
            return None
        if role == self.DISPLAY_ROLE:
            return format_value(self.value(row, index.column()))
        if role == self.TOOLTIP_ROLE:
            return repr(self.value(row, index.column()))
        if role == self.TEXT_ALIGNMENT_ROLE and index.column() != shots.COL_FILEPATH:
            return self._alignment
        return None

    def headerData(self, section, orientation, role=DISPLAY_ROLE):
        shots = self.shots
        if orientation == QtCore.Qt.Vertical:
            if role == self.DISPLAY_ROLE:
                return shots.row_label(section)
            return None
        if section == shots.COL_STATUS:
            if role == self.DECORATION_ROLE:
                return self._status_icon
            if role == self.TOOLTIP_ROLE:
                return 'status/progress of single-shot analysis'
            return None
        if role == self.DISPLAY_ROLE or role == self.TOOLTIP_ROLE:
            return '\n'.join(shots.column_names[section]).strip()
        return None


class DataFrameModel(QtCore.QObject):

    COL_STATUS = 0
    COL_FILEPATH = 1

    ROLE_STATUS_PERCENT = QtCore.Qt.UserRole + 1
    ROLE_STATE = QtCore.Qt.UserRole + 2

    # In device-independent pixels:
    ROW_HEIGHT = 20

    columns_changed = Signal()

    def __init__(self, app, view, exp_config):
        super().__init__()
        self.app = app
        self._view = view
        self.exp_config = exp_config

        # Check if integer indexing is to be used
        try:
            self.integer_indexing = self.exp_config.getboolean('lyse', 'integer_indexing')
        except (LabConfig.NoOptionError, LabConfig.NoSectionError):
            self.integer_indexing = False

        # This dataframe will contain all the scalar data
        # from the shot files that are currently open:
        index = pandas.MultiIndex.from_tuples([('filepath', '')])
        self.dataframe = pandas.DataFrame({'filepath': []}, columns=index)
        # How many levels the dataframe's multiindex has:
        self.nlevels = self.dataframe.columns.nlevels

        # The file path, the percentage of its single-shot routines run while they run,
        # and the state of its analysis, of each shot by row (see STATE_TOOLTIPS):
        self.filepaths = []
        self.status_percent = []
        self.states = []
        self.row_number_by_filepath = {}
        # No row before this one is not done:
        self._first_incomplete = 0
        # The shot whose single-shot routines are running, the order in which the shots
        # awaiting multishot came to it, and the shots to set not done once their
        # attempt ends:
        self.analysing = None
        self._awaiting_since = {}
        self._awaiting_count = itertools.count()
        self._rerun_after_attempt = set()
        # The shots a failed pass left awaiting the next:
        self._pass_failed = set()

        # Column indices to names and vice versa for fast lookup:
        self.column_indices = {'__status': self.COL_STATUS, ('filepath', ''): self.COL_FILEPATH}
        self.column_names = {self.COL_STATUS: '__status', self.COL_FILEPATH: ('filepath', '')}
        self.columns_visible = {self.COL_STATUS: True, self.COL_FILEPATH: True}

        # Whether or not a deleted column was visible at the time it was deleted (by name):
        self.deleted_columns_visible = {}

        self._model = ShotsTableModel(self)
        self._header = HorizontalHeaderViewWithWidgets(self._model)
        # Column widths are fitted to the rows that are visible:
        self._header.setResizeContentsPrecision(0)
        self._vertheader = QtWidgets.QHeaderView(QtCore.Qt.Vertical)
        self._vertheader.setSectionResizeMode(QtWidgets.QHeaderView.Fixed)
        self._vertheader.setDefaultSectionSize(self.ROW_HEIGHT)

        # Smaller font for the headers:
        font = self._header.font()
        font.setPixelSize(11)
        self._header.setFont(font)
        self._vertheader.setFont(monospace_font(11))

        self._vertheader.setHighlightSections(True)
        self._vertheader.setSectionsClickable(True)
        self._view.setModel(self._model)
        self._view.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        self._view.setHorizontalHeader(self._header)
        self._view.setVerticalHeader(self._vertheader)
        self._delegate = lyse.widgets.ItemDelegate(self.app, self._model, self.COL_STATUS, self.ROLE_STATUS_PERCENT)
        self._view.setItemDelegate(self._delegate)
        self._view.setSelectionBehavior(QtWidgets.QTableView.SelectRows)
        self._view.setContextMenuPolicy(QtCore.Qt.CustomContextMenu)

        self._view.setColumnWidth(self.COL_STATUS, 70)
        self._view.setColumnWidth(self.COL_FILEPATH, 100)

        # Make the actions for the context menu:
        self.action_remove_selected = QtWidgets.QAction(
            Icon(':qtutils/fugue/minus'), 'Remove selected shots',  self._view)

        self.connect_signals()

    def connect_signals(self):
        self._view.customContextMenuRequested.connect(self.on_view_context_menu_requested)
        self.action_remove_selected.triggered.connect(self.on_remove_selection)

    def on_remove_selection(self):
        self.remove_selection()

    def _selected_rows(self):
        """Return the sorted numbers of the selected rows"""
        selected_rows = set()
        for selection_range in self._view.selectionModel().selection():
            selected_rows.update(range(selection_range.top(), selection_range.bottom() + 1))
        return sorted(selected_rows)

    def selected_filepaths(self):
        return [self.filepaths[row] for row in self._selected_rows()]

    def _rows_named(self, filepaths):
        """The sorted rows of these shots. Raise ValueError naming any that is not in
        the table."""
        unknown = set(filepaths) - self.row_number_by_filepath.keys()
        if unknown:
            raise ValueError(f'Not a shot in lyse: {", ".join(sorted(unknown))}')
        return sorted(self.row_number_by_filepath[filepath] for filepath in filepaths)

    def shot_states(self, filepaths):
        """The state of each shot's analysis, in the order of filepaths."""
        self._rows_named(filepaths)
        return [self.states[self.row_number_by_filepath[filepath]] for filepath in filepaths]

    def remove_selection(self, confirm=True):
        filepaths = self.selected_filepaths()
        if not filepaths:
            return
        if confirm and not self.app.question_dialog(f"Remove {len(filepaths)} shots?"):
            return
        self.remove_shots(filepaths)

    def remove_shots(self, filepaths):
        selected_rows = self._rows_named(filepaths)
        if not selected_rows:
            return
        # The view reads the dataframe as it draws, so it changes with the lists inside
        # one notification: a row removal for one block, else a reset, since a removal
        # per block would copy the dataframe once per block.
        first, last = selected_rows[0], selected_rows[-1]
        contiguous = last - first + 1 == len(selected_rows)
        if contiguous:
            self._model.beginRemoveRows(QtCore.QModelIndex(), first, last)
        else:
            self._model.beginResetModel()
        self.dataframe = self.dataframe.drop(selected_rows)
        self.dataframe.index = pandas.Index(range(len(self.dataframe)))
        removed = set(selected_rows)
        for filepath in filepaths:
            self._awaiting_since.pop(filepath, None)
            self._rerun_after_attempt.discard(filepath)
            self._pass_failed.discard(filepath)
        if self.analysing in filepaths:
            self.analysing = None
        for values in [self.filepaths, self.status_percent, self.states]:
            values[:] = [value for row, value in enumerate(values) if row not in removed]
        if contiguous:
            self._model.endRemoveRows()
        else:
            self._model.endResetModel()
        self.row_number_by_filepath = {filepath: row for row, filepath in enumerate(self.filepaths)}
        self._first_incomplete = 0

    def _set_state(self, row, state):
        self.states[row] = state
        if state == 'not done':
            self.status_percent[row] = 0
            self._first_incomplete = min(self._first_incomplete, row)
        index = self._model.index(row, self.COL_STATUS)
        self._model.dataChanged.emit(index, index)

    def mark_not_done(self, filepaths):
        """Set shots not done, so that they are analysed again; one whose attempt is
        under way, once it ends."""
        for row in self._rows_named(filepaths):
            filepath = self.filepaths[row]
            # A file once unreadable may be back, undeleted or after a network glitch:
            if self.states[row] == 'deleted' and not os.path.exists(filepath):
                continue
            # A shot a failed pass left awaiting the next has no attempt under way:
            awaiting = self.states[row] == 'awaiting multishot' and filepath not in self._pass_failed
            if filepath == self.analysing or awaiting:
                self._rerun_after_attempt.add(filepath)
            else:
                self._awaiting_since.pop(filepath, None)
                self._pass_failed.discard(filepath)
                self._set_state(row, 'not done')

    def failed_filepaths(self):
        """The shots whose last attempt failed, stopped or was aborted, or whose last
        multishot pass failed."""
        return [filepath for filepath, state in zip(self.filepaths, self.states)
                if state in ('failed', 'stopped', 'aborted') or filepath in self._pass_failed]

    @inmain_decorator()
    def start_attempt(self, filepath):
        self.analysing = filepath

    @inmain_decorator()
    def await_multishot(self, filepath):
        """Leave a shot whose single-shot routines have run awaiting the next pass."""
        self.analysing = None
        self._pass_failed.discard(filepath)
        if filepath in self.row_number_by_filepath:
            self._awaiting_since[filepath] = next(self._awaiting_count)
            self._set_state(self.row_number_by_filepath[filepath], 'awaiting multishot')

    @inmain_decorator()
    def awaiting_multishot(self):
        """The shots awaiting multishot, in the order they came to it."""
        return sorted(self._awaiting_since, key=self._awaiting_since.get)

    @inmain_decorator()
    def end_attempt(self, filepath, state):
        """End a shot's attempt in state, which leaves it awaiting multishot after a
        failed pass, or not done if a re-run was asked for during the attempt."""
        self.analysing = None
        if filepath not in self.row_number_by_filepath:
            return
        if filepath in self._rerun_after_attempt:
            self._rerun_after_attempt.discard(filepath)
            state = 'not done'
        if state == 'awaiting multishot':
            self._pass_failed.add(filepath)
        else:
            self._pass_failed.discard(filepath)
            self._awaiting_since.pop(filepath, None)
        self._set_state(self.row_number_by_filepath[filepath], state)

    def on_view_context_menu_requested(self, point):
        menu = QtWidgets.QMenu(self._view)
        menu.addAction(self.action_remove_selected)
        menu.exec(QtGui.QCursor.pos())

    def on_double_click(self, index):
        shot_filepath = self.filepaths[index.row()]
        try:
            open_file(shot_filepath, 'hdf5_viewer', self.exp_config)
        except Exception as e:
            self.app.error_dialog("Unable to launch hdf5 viewer. Error was: %s. The hdf5 viewer is set in the [programs] section of %s." %
                (str(e), self.exp_config.config_path))
        
    def set_columns_visible(self, columns_visible):
        self.columns_visible = columns_visible
        for column_index, visible in columns_visible.items():
            self._view.setColumnHidden(column_index, not visible)

    def update_column_levels(self):
        """Pad the column names with empty strings to match the dataframe after its
        multiindex gains levels, which concat_with_padding() never removes."""
        extra_levels = self.dataframe.columns.nlevels - self.nlevels
        if extra_levels > 0:
            self.nlevels = self.dataframe.columns.nlevels
            column_indices = {}
            column_names = {}
            for column_name in self.column_indices:
                if not isinstance(column_name, tuple):
                    # It's one of our special columns
                    new_column_name = column_name
                else:
                    new_column_name = column_name + ('',) * extra_levels
                column_index = self.column_indices[column_name]
                column_indices[new_column_name] = column_index
                column_names[column_index] = new_column_name
            self.column_indices = column_indices
            self.column_names = column_names

    @inmain_decorator()
    def mark_deleted(self, filepath, warn=True):
        """Mark a shot whose file is gone, which ends any attempt of it and keeps it
        out of analysis, warning that it vanished unless warn is False."""
        if self.analysing == filepath:
            self.analysing = None
        # On the GUI thread, so the row cannot be removed between this check and its use:
        row_number = self.row_number_by_filepath.get(filepath)
        if row_number is None or self.states[row_number] == 'deleted':
            return
        self._awaiting_since.pop(filepath, None)
        self._pass_failed.discard(filepath)
        self._set_state(row_number, 'deleted')
        if warn:
            self.app.output_box.output(f'Warning: Shot deleted from disk or no longer readable {filepath}\n', red=True)

    @inmain_decorator()
    def infer_objects(self):
        """Convert columns in the dataframe with dtype 'object' into compatible, more
        specific types, if possible. This improves pickling performance and ensures
        multishot analysis code does not encounter columns with dtype 'object' for
        non-mixed numerical data, which it might choke on.
        """
        self.dataframe = self.dataframe.infer_objects()

    @inmain_decorator()
    def update_rows(self, updated_data):
        """Update the dataframe and Qt model with results saved by an analysis
        routine. updated_data is a dict mapping shot filepaths to dicts of
        {(group, name): value}."""
        # Flatten dict values into extra column levels, as get_dataframe_from_shot()
        # does when the shot is loaded from disk, and collect the rows and values of
        # each column:
        rows_by_column = {}
        for filepath, row_data in updated_data.items():
            try:
                row_number = self.row_number_by_filepath[filepath]
            except KeyError:
                # The row has been deleted, so its results cannot be recorded, and
                # a result not saved to the shot file is kept nowhere else:
                if row_data:
                    self.app.output_box.output(
                        f'Warning: results not recorded for {filepath}, which has no row\n', red=True)
                continue
            for (group, name), value in row_data.items():
                for key, flat_value in flatten_dict({name: value}, keys=(group,)).items():
                    rows, values = rows_by_column.setdefault(key, ([], []))
                    rows.append(row_number)
                    values.append(flat_value)
        if not rows_by_column:
            return
        nlevels = max([self.nlevels] + [len(key) for key in rows_by_column])
        if nlevels > self.nlevels:
            self.dataframe = pad_columns(self.dataframe, nlevels)
            self._model.dataframe_changed()
            self.update_column_levels()
        rows_by_column = {
            key + ('',) * (self.nlevels - len(key)): rows_and_values
            for key, rows_and_values in rows_by_column.items()
        }
        for column_name, (rows, values) in rows_by_column.items():
            self._set_dataframe_values(column_name, rows, values)

        new_column_numbers = self._add_new_columns()
        self._resize_new_columns(new_column_numbers)
        all_rows = [row for rows, _ in rows_by_column.values() for row in rows]
        column_numbers = [self.column_indices[name] for name in rows_by_column]
        self._model.dataChanged.emit(self._model.index(min(all_rows), min(column_numbers)),
                                     self._model.index(max(all_rows), max(column_numbers)))

    def _set_dataframe_values(self, column_name, rows, values):
        """Set the values of one dataframe column in the given rows"""
        if column_name not in self.dataframe.columns:
            # Create the column from its first value, so that it gets the dtype that
            # pandas infers for that value:
            self._set_dataframe_value(rows[0], column_name, values[0])
            rows, values = rows[1:], values[1:]
            if not rows:
                return
        column = self.dataframe[column_name]
        if column.dtype == object:
            column_values = column.to_numpy(copy=True)
            for row_number, value in zip(rows, values):
                column_values[row_number] = value
            self.dataframe[column_name] = pandas.Series(column_values, index=column.index, dtype=object)
            return
        if len(rows) == 1:
            self._set_dataframe_value(rows[0], column_name, values[0])
            return
        new_values = pandas.Series(values, index=rows)
        if new_values.dtype != object:
            try:
                self.dataframe.loc[rows, column_name] = new_values
                return
            except (ValueError, TypeError):
                pass
        for row_number, value in zip(rows, values):
            self._set_dataframe_value(row_number, column_name, value)

    def _set_dataframe_value(self, row_number, column_name, value):
        try:
            if column_name in self.dataframe.columns:
                self.dataframe.at[row_number, column_name] = value
            else:
                # pandas deprecates creating a column with .at:
                self.dataframe.loc[row_number, column_name] = value
        except (ValueError, TypeError):
            # did the column not already exist when we tried to set an iterable?
            if not column_name in self.dataframe.columns:
                # create it with a non-iterable and then overwrite with the iterable value:
                self.dataframe.loc[row_number, column_name] = None
            elif (
                pandas.api.types.is_integer_dtype(self.dataframe[column_name].dtype)
                and isinstance(value, (float, np.floating))
            ):
                # A float in an integer column - convert the column to float:
                self.dataframe[column_name] = self.dataframe[column_name].astype('float64')
            else:
                # Incompatible datatype - convert the datatype of the column to
                # 'object'
                self.dataframe[column_name] = self.dataframe[column_name].astype('object')
            # Now that the column exists and has a compatible dtype, we can set the value:
            self.dataframe.at[row_number, column_name] = value

    def _add_new_columns(self):
        """Add Qt model columns for the dataframe columns that the model does not
        have. Returns the numbers of the new columns."""
        new_column_names = set(self.dataframe.columns) - set(self.column_names.values())
        if not new_column_names:
            return []
        new_columns_start = len(self.column_names)
        new_column_numbers = list(range(new_columns_start, new_columns_start + len(new_column_names)))
        self._model.beginInsertColumns(QtCore.QModelIndex(), new_column_numbers[0], new_column_numbers[-1])
        for column_number, column_name in zip(new_column_numbers, sorted(new_column_names)):
            self.column_names[column_number] = column_name
            self.column_indices[column_name] = column_number
            # Restore the former visibility of this column if we've seen one with its
            # name before. New columns are visible by default:
            self.columns_visible[column_number] = self.deleted_columns_visible.get(column_name, True)
        self._model.endInsertColumns()
        for column_number in new_column_numbers:
            if not self.columns_visible[column_number]:
                self._view.setColumnHidden(column_number, True)
        return new_column_numbers

    def _remove_defunct_columns(self):
        """Remove the Qt model columns that are no longer in the dataframe. This happens
        when new shots are added after all shots have been removed."""
        defunct_column_names = (set(self.column_names.values()) - set(self.dataframe.columns)
                                - {self.column_names[self.COL_STATUS], self.column_names[self.COL_FILEPATH]})
        if not defunct_column_names:
            return
        defunct_column_indices = [self.column_indices[column_name] for column_name in defunct_column_names]
        for column_number in sorted(defunct_column_indices, reverse=True):
            # Remove columns from the Qt model. In reverse order so that
            # removals do not change the position of columns yet to be
            # removed.
            self._model.beginRemoveColumns(QtCore.QModelIndex(), column_number, column_number)
            # Save whether or not the column was visible when it was
            # removed (so that if it is re-added the visibility will be retained):
            self.deleted_columns_visible[self.column_names[column_number]] = self.columns_visible[column_number]
            # Renumber the columns after the removed one:
            self.column_names = {index - (index > column_number): name
                                 for index, name in self.column_names.items() if index != column_number}
            self.columns_visible = {index - (index > column_number): visible
                                    for index, visible in self.columns_visible.items() if index != column_number}
            self._model.dataframe_changed()
            self._model.endRemoveColumns()
        # Update the inverse mapping of self.column_names:
        self.column_indices = {name: index for index, name in self.column_names.items()}
        self.columns_changed.emit()

    def _resize_new_columns(self, new_column_numbers):
        for column_number in new_column_numbers:
            self._view.resizeColumnToContents(column_number)
        if new_column_numbers:
            self.columns_changed.emit()

    @inmain_decorator()
    def set_status_percent(self, filepath, status_percent):
        try:
            row_number = self.row_number_by_filepath[filepath]
        except KeyError:
            # Row has been deleted, nothing to do here:
            return
        self.status_percent[row_number] = status_percent
        index = self._model.index(row_number, self.COL_STATUS)
        self._model.dataChanged.emit(index, index)

    def row_label(self, row):
        """Return the vertical header text of a row: the row number, followed by the
        file name, or by the sequence index, run number and run repeat if integer
        indexing is used"""
        n_digits = len(str(len(self.filepaths)))
        label = '{}. '.format(str(row).rjust(n_digits))
        if self.integer_indexing:
            header_strings = []
            for name in ['sequence_index', 'run number', 'run repeat']:
                value = self.dataframe.at[row, (name,) + ('',) * (self.nlevels - 1)]
                if pandas.notna(value):
                    header_strings.append('{:04d}'.format(int(value)))
                else:
                    header_strings.append('----')
            return label + ' | '.join(header_strings)
        return label + os.path.splitext(os.path.basename(self.filepaths[row]))[0]

    @inmain_decorator()
    def add_files(self, filepaths, new_row_data, sources, done=False):
        """Add files to the dataframe model. new_row_data is a dataframe
        containing the new rows, and sources says where each came from, as
        shot_received tells the plugins."""

        to_add = []
        to_add_set = set()
        source_by_filepath = dict(zip(filepaths, sources))

        for filepath in filepaths:
            if filepath in self.row_number_by_filepath or filepath in to_add_set:
                self.app.output_box.output('Warning: Ignoring duplicate shot %s\n' % filepath, red=True)
                if new_row_data is not None:
                    df_row_index = np.where(new_row_data['filepath'].values == filepath)
                    new_row_data = new_row_data.drop(new_row_data.index[df_row_index[0]])
                    new_row_data.index = pandas.Index(range(len(new_row_data)))
            else:
                to_add.append(filepath)
                to_add_set.add(filepath)

        assert len(new_row_data) == len(to_add)

        if not to_add:
            return

        # Whether shots were already being added when this method was called:
        adding_shots = self.app.filebox.add_shots_start_time is not None

        # Update the dataframe:
        self.dataframe = concat_with_padding(self.dataframe, new_row_data)
        self._model.dataframe_changed()
        self.update_column_levels()
        self._remove_defunct_columns()

        self.app.filebox.set_add_shots_progress(None, None, "updating filebox")

        new_column_numbers = self._add_new_columns()
        first_new_row = len(self.filepaths)
        self._model.beginInsertRows(QtCore.QModelIndex(), first_new_row, first_new_row + len(to_add) - 1)
        for row_number, filepath in enumerate(to_add, first_new_row):
            self.filepaths.append(filepath)
            self.status_percent.append(0)
            self.states.append('done' if done else 'not done')
            self.row_number_by_filepath[filepath] = row_number
        if not done:
            self._first_incomplete = min(self._first_incomplete, first_new_row)
        self._model.endInsertRows()
        self._resize_new_columns(new_column_numbers)
        # On the GUI thread, from which the analysis loop takes its next shot, so before it
        # can take one of these:
        for filepath in to_add:
            self.app.filebox.fire_event('shot_received', filepath, source_by_filepath[filepath])

        if not adding_shots:
            self.app.filebox.set_add_shots_progress(None, None, None)

    @inmain_decorator()
    def get_first_incomplete(self):
        """Returns the filepath of the first shot in the model that has not
        been analysed"""
        while (self._first_incomplete < len(self.states)
               and self.states[self._first_incomplete] != 'not done'):
            self._first_incomplete += 1
        if self._first_incomplete < len(self.filepaths):
            return self.filepaths[self._first_incomplete]
        return None

class FileBox:

    # Time for which shots must have been being added before the progress bar is shown,
    # in seconds:
    ADD_SHOTS_PROGRESS_DELAY = 0.5

    def __init__(self, app, container, exp_config, to_singleshot, from_singleshot, to_multishot, from_multishot):
        self.app = app

        self.exp_config = exp_config
        self.to_singleshot = to_singleshot
        self.to_multishot = to_multishot
        self.from_singleshot = from_singleshot
        self.from_multishot = from_multishot

        self.logger = logging.getLogger('lyse.FileBox')
        self.logger.info('starting')

        loader = UiLoader()
        loader.registerCustomWidget(TableView)
        self.ui = loader.load(os.path.join(lyse.utils.LYSE_DIR, 'user_interface/filebox.ui'))
        self.ui.progressBar_add_shots.hide()
        # The monotonic time at which the shots being added began to be added, or None:
        self.add_shots_start_time = None
        self.add_shots_progress_timer = QtCore.QTimer()
        self.add_shots_progress_timer.setSingleShot(True)
        self.add_shots_progress_timer.timeout.connect(self.ui.progressBar_add_shots.show)
        container.addWidget(self.ui)
        self.shots_model = DataFrameModel(self.app, self.ui.tableView, self.exp_config)
        set_auto_scroll_to_end(self.ui.tableView.verticalScrollBar())
        self.edit_columns_dialog = EditColumns(self, self.shots_model.column_names, self.shots_model.columns_visible)

        self.last_opened_shots_folder = self.exp_config.get('paths', 'experiment_shot_storage')

        self.connect_signals()

        self.analysis_paused = False
        self.multishot_required = False
        # Whether the Run button was clicked, which runs the multishot routines even if
        # analysis is paused:
        self.multishot_requested = False
        # Whether a shot's later single-shot routines are skipped after one fails:
        self.stop_on_failure = False
        # The single-shot routines' outcomes of the shots awaiting multishot, and the
        # names of the outcomes lyse last wrote for each shot, by phase:
        self._singleshot_outcomes = {}
        self._recorded_outcomes = {}
        # Set as lyse quits, after which no plugin is called and no analysis starts:
        self.quitting = False

        # An Event to let the analysis thread know to check for shots that
        # need analysing, rather than using a time.sleep:
        self.analysis_pending = threading.Event()

        # A queue for storing incoming files from the ZMQ server so
        # the server can keep receiving files even if analysis is slow
        # or paused:
        self.incoming_queue = queue.Queue()

        # Start the thread to handle incoming files, and store them in
        # a buffer if processing is paused:
        self.incoming = threading.Thread(target=self.incoming_buffer_loop)
        self.incoming.daemon = True
        self.incoming.start()

        self.analysis = threading.Thread(target = self.analysis_loop)
        self.analysis.daemon = True
        self.analysis.start()

    def connect_signals(self):
        self.ui.pushButton_edit_columns.clicked.connect(self.on_edit_columns_clicked)
        self.shots_model.columns_changed.connect(self.on_columns_changed)
        self.ui.toolButton_add_shots.clicked.connect(self.on_add_shot_files_clicked)
        self.ui.toolButton_remove_shots.clicked.connect(self.shots_model.on_remove_selection)
        self.ui.tableView.doubleLeftClicked.connect(self.shots_model.on_double_click)
        self.ui.pushButton_analysis_running.toggled.connect(self.on_analysis_running_toggled)
        self.ui.pushButton_abort.clicked.connect(self.abort)
        self.ui.checkBox_stop_on_failure.toggled.connect(self.on_stop_on_failure_toggled)
        self.ui.toolButton_rerun_failed.clicked.connect(self.rerun_failed)
        self.ui.pushButton_mark_as_not_done.clicked.connect(
            lambda: self.rerun_shots(self.shots_model.selected_filepaths()))
        self.app.multishot_routinebox.ui.toolButton_run_routines.clicked.connect(self.run_multishot)

    def on_edit_columns_clicked(self):
        self.edit_columns_dialog.show()

    def on_columns_changed(self):
        column_names = self.shots_model.column_names
        columns_visible = self.shots_model.columns_visible
        self.edit_columns_dialog.update_columns(column_names, columns_visible)

    def on_add_shot_files_clicked(self):
        shot_files = file_dialogs.open_files(
            self.ui,
            'Select shot files',
            self.last_opened_shots_folder,
            "HDF5 files (*.h5)",
        )
        if not shot_files:
            # User cancelled selection
            return
        # In the order of their sequences and repeats, whatever order the dialog gives:
        shot_files = sorted(shot_files, key=natural_sort_key)

        # Save the containing folder for use next time we open the dialog box:
        self.last_opened_shots_folder = os.path.dirname(shot_files[0])
        for filepath in shot_files:
            self.incoming_queue.put((filepath, 'dialog'))

    def abort(self):
        self.set_analysis_paused(True)
        self.app.singleshot_routinebox.abort()
        self.app.multishot_routinebox.abort()

    def on_analysis_running_toggled(self, pressed):
        self.analysis_paused = pressed
        if not pressed:
            self.analysis_pending.set()

    def on_stop_on_failure_toggled(self, checked):
        self.stop_on_failure = checked

    @inmain_decorator()
    def set_stop_on_failure(self, stop):
        # Through the checkbox, whose slot sets self.stop_on_failure:
        self.ui.checkBox_stop_on_failure.setChecked(stop)

    def rerun_failed(self):
        self.rerun_shots(self.shots_model.failed_filepaths())

    def rerun_shots(self, filepaths):
        self.shots_model.mark_not_done(filepaths)
        # Let the analysis loop know to look for these shots:
        self.analysis_pending.set()

    def run_multishot(self):
        self.multishot_required = True
        self.multishot_requested = True
        self.analysis_pending.set()

    def set_columns_visible(self, columns_visible):
        self.shots_model.set_columns_visible(columns_visible)

    @inmain_decorator()
    def set_add_shots_progress(self, completed, total, message):
        """Update the progress bar of adding shots, or hide it if completed equals total
        and message is None. The progress bar is shown once shots have been being added
        for ADD_SHOTS_PROGRESS_DELAY seconds."""
        progress_bar = self.ui.progressBar_add_shots
        progress_bar.setFormat("Adding shots: [{}] %v/%m (%p%)".format(message))
        if completed == total and message is None:
            self.add_shots_start_time = None
            self.add_shots_progress_timer.stop()
            progress_bar.hide()
            return
        if total is not None:
            progress_bar.setMaximum(total)
        if completed is not None:
            progress_bar.setValue(completed)
        if self.add_shots_start_time is None:
            self.add_shots_start_time = time.monotonic()
            self.add_shots_progress_timer.start(int(1000 * self.ADD_SHOTS_PROGRESS_DELAY))
        elif progress_bar.isHidden():
            # The timer does not fire while the main thread is adding shots:
            if time.monotonic() - self.add_shots_start_time >= self.ADD_SHOTS_PROGRESS_DELAY:
                self.add_shots_progress_timer.stop()
                progress_bar.show()
        if not progress_bar.isHidden() and completed is None and total is None and message is not None:
            # Ensure a repaint when only the message changes:
            progress_bar.repaint()

    def incoming_buffer_loop(self):
        """Add the shots in the incoming queue to the dataframe, in batches.

        Shots pile up in the queue until this loop reads them, so a client
        submitting them never waits. Errors cannot be returned to it, and an
        experiment should not stop on analysis errors anyway, so they are raised
        here for the user."""
        logger = logging.getLogger('lyse.FileBox.incoming')
        # HDF5 prints errors for things that aren't errors. They are silenced per
        # thread, and in the main thread on importing h5py, so silence them here too.
        h5py._errors.silence_errors()
        n_shots_added = 0
        while True:
            try:
                filepaths = []
                # The first source given for a path, which a later one does not replace:
                sources = {}
                filepath, source = self.incoming_queue.get()
                filepaths.append(filepath)
                sources.setdefault(filepath, source)
                if self.incoming_queue.qsize() == 0:
                    # Wait momentarily in case more arrive so we can batch process them:
                    time.sleep(0.1)
                # Batch process to decrease number of dataframe concatenations:
                batch_size = len(self.shots_model.dataframe) // 3 + 1
                while True:
                    try:
                        filepath, source = self.incoming_queue.get(False)
                    except queue.Empty:
                        break
                    else:
                        filepaths.append(filepath)
                        sources.setdefault(filepath, source)
                        if len(filepaths) >= batch_size:
                            break
                logger.info('adding:\n%s', '\n'.join(filepaths))
                if n_shots_added == 0:
                    total_shots = self.incoming_queue.qsize() + len(filepaths)
                    self.set_add_shots_progress(1, total_shots, "reading shot files")

                # Remove duplicates from the list (preserving order) in case the
                # client sent the same filepath multiple times:
                filepaths = sorted(set(filepaths), key=filepaths.index) # Inefficient but readable
                # We open the HDF5 files here outside the GUI thread so as not to hang the GUI:
                dataframes = []
                indices_of_files_skipped = []
                for i, filepath in enumerate(filepaths):
                    try:
                        dataframe = get_dataframe_from_shot(filepath, warn=lambda message:
                            self.app.output_box.output(f'Warning: {message}\n', red=True))
                        # runmanager submits a shot once BLACS has run it, so it is added then:
                        if dataframe['run time'].isna().values.all():
                            self.app.output_box.output(
                                f'Warning: Ignoring shot file that has not been run {filepath}\n', red=True)
                            indices_of_files_skipped.append(i)
                        else:
                            dataframes.append(dataframe)
                    except IOError:
                        self.app.output_box.output(f'Warning: Ignoring shot file not found or not readable {filepath}\n', red=True)
                        indices_of_files_skipped.append(i)
                    except Exception as e:
                        self.app.output_box.output(
                            f'Warning: Ignoring shot file {filepath}: {type(e).__name__}: {e}\n', red=True)
                        indices_of_files_skipped.append(i)
                    n_shots_added += 1
                    shots_remaining = self.incoming_queue.qsize()
                    total_shots = n_shots_added + shots_remaining + len(filepaths) - (i + 1)
                    self.set_add_shots_progress(n_shots_added, total_shots, "reading shot files")
                self.set_add_shots_progress(n_shots_added, total_shots, "concatenating dataframes")
                if dataframes:
                    new_row_data = concat_with_padding(*dataframes)
                else:
                    new_row_data = None

                # Do not add the shots that could not be read. Reverse
                # loop so that removing an item doesn't change the indices of
                # subsequent removals:
                for i in reversed(indices_of_files_skipped):
                    del filepaths[i]
                if filepaths:
                    self.shots_model.add_files(filepaths, new_row_data, [sources[path] for path in filepaths])
                    # Let the analysis loop know to look for new shots:
                    self.analysis_pending.set()
                if shots_remaining == 0:
                    self.set_add_shots_progress(n_shots_added, total_shots, None)
                    n_shots_added = 0

            except Exception:
                # Keep this incoming loop running at all costs, but make the
                # otherwise uncaught exception visible to the user:
                self.set_add_shots_progress(None, None, None)
                n_shots_added = 0
                zprocess.raise_exception_in_thread(sys.exc_info())

    def analysis_loop(self):
        logger = logging.getLogger('lyse.FileBox.analysis_loop')
        # HDF5 prints errors for things that aren't errors. They are silenced per
        # thread, and in the main thread on importing h5py, so silence them here too.
        h5py._errors.silence_errors()
        while True:
            try:
                self.analysis_pending.wait()
                self.analysis_pending.clear()
                at_least_one_shot_analysed = False
                multishot_failed = False
                while True:
                    if not self.analysis_paused and not self.quitting:
                        filepath = self.shots_model.get_first_incomplete()
                        if filepath is not None:
                            logger.info('analysing: %s', filepath)
                            self.do_singleshot_analysis(filepath)
                            at_least_one_shot_analysed = True
                        # Multishot analysis runs once the queue drains, not once per shot, so a
                        # multishot routine must process every unseen row, not only the last.
                        if filepath is None and at_least_one_shot_analysed:
                            self.multishot_required = True
                        if filepath is None:
                            break
                        # Analysis may have been paused during the shot:
                        if self.multishot_required and (self.multishot_requested or not self.analysis_paused):
                            logger.info('doing multishot analysis')
                            multishot_failed = not self.do_multishot_analysis()
                    else:
                        logger.info('analysis is paused')
                        break
                # A pass that has just failed runs again with the next shots, or
                # on resuming if analysis is paused, rather than now:
                if (self.multishot_required and not multishot_failed
                        and (self.multishot_requested or not self.analysis_paused)):
                    logger.info('doing multishot analysis')
                    self.do_multishot_analysis()
            except Exception:
                etype, value, tb = sys.exc_info()
                orig_exception = ''.join(traceback.format_exception_only(etype, value))
                message = ('Analysis loop encountered unexpected exception. ' +
                           'This is a bug and should be reported. The analysis ' +
                           'loop is continuing, but lyse may be in an inconsistent state. '
                           'Restart lyse, or continue at your own risk. '
                           'Original exception was:\n\n' + orig_exception)
                # Raise the exception in a thread so we can keep running
                zprocess.raise_exception_in_thread((RuntimeError, RuntimeError(message), tb))
                self.set_analysis_paused(True)


    @inmain_decorator()
    def set_analysis_paused(self, paused):
        # Through the button, whose slot sets self.analysis_paused:
        self.ui.pushButton_analysis_running.setChecked(paused)

    def fire_event(self, name, *args):
        """Call the plugins' handlers of the event, unless lyse is quitting."""
        if not self.quitting:
            lyse.plugins.manager.fire_event(name, *args)

    def do_singleshot_analysis(self, filepath):
        """Begin the shot's attempt: run its single-shot routines, then leave it
        awaiting multishot, or, with no multishot routine active, end the attempt."""
        # A shot file deleted before analysis would make user code raise and fill the
        # output box with errors, so check first. It can still be deleted after this
        # check, which the 'error' branch below handles.
        if not os.path.exists(filepath):
            self.shots_model.mark_deleted(filepath)
            return
        self.shots_model.start_attempt(filepath)
        self.to_singleshot.put(filepath)
        previous_status_percent = None
        outcomes = {}
        while True:
            signal, status_percent, updated_data, outcome = self.from_singleshot.get()
            if updated_data:
                self.shots_model.update_rows(updated_data)
            outcomes.update(outcome)
            # Update the status percent for the the row on which analysis is actually
            # running:
            if status_percent is not None and status_percent != previous_status_percent:
                self.shots_model.set_status_percent(filepath, status_percent)
                previous_status_percent = status_percent
            if signal == 'progress':
                continue
            if signal not in ('done', 'error'):
                raise ValueError(f'invalid signal {signal}')
            break
        if signal == 'error' and not os.path.exists(filepath):
            # An error is no surprise once the file has gone:
            self.shots_model.mark_deleted(filepath)
            return
        # A new attempt's outcomes replace all those lyse wrote for the shot before:
        previous = self._recorded_outcomes.get(filepath, {})
        replaced = previous.get('singleshot', set()) | previous.get('multishot', set())
        self.record_outcomes(filepath, outcomes, replaced)
        self._recorded_outcomes[filepath] = {'singleshot': set(outcomes), 'multishot': set()}
        self.fire_event('singleshot_done', filepath, outcomes)
        if any(routine.enabled() for routine in self.app.multishot_routinebox.routines):
            self._singleshot_outcomes[filepath] = outcomes
            self.shots_model.await_multishot(filepath)
        else:
            self.shots_model.end_attempt(filepath, outcome_state(outcomes))
            self.fire_event('analysis_done', filepath, outcomes)

    def record_outcomes(self, filepath, outcomes, replaced):
        """Put outcomes in the shot's row and, under its file's lock, in its file's
        results/lyse group, in place of the outcomes named in replaced, which lyse put
        there before, and no others. A failure to write the file is reported, and the
        row keeps the outcomes."""
        row_data = {('lyse', name): np.nan for name in replaced}
        row_data.update({('lyse', name): outcome for name, outcome in outcomes.items()})
        self.shots_model.update_rows({filepath: row_data})
        try:
            with h5py.File(filepath, 'a') as f:
                group = f.require_group('results/lyse')
                for name in replaced:
                    if name in group.attrs:
                        del group.attrs[name]
                for name, outcome in outcomes.items():
                    group.attrs[name] = outcome
        except Exception as e:
            self.app.output_box.output(
                f'Warning: The routines\' outcomes were not written into {filepath}: {e}\n', red=True)

    def do_multishot_analysis(self):
        """Run a pass on the shots awaiting multishot, which ends their attempts. After
        a failed pass they await the next one."""
        self.multishot_requested = False
        paths = []
        for filepath in self.shots_model.awaiting_multishot():
            if os.path.exists(filepath):
                paths.append(filepath)
            else:
                # Its attempt ends here, and the routines are not given its path:
                self.shots_model.mark_deleted(filepath)
                self.fire_event('analysis_done', filepath, self._singleshot_outcomes.pop(filepath, {}))
        self.to_multishot.put(paths)
        outcomes = {}
        while True:
            signal, status_percent, updated_data, outcome = self.from_multishot.get()
            if updated_data:
                self.shots_model.update_rows(updated_data)
            outcomes.update(outcome)
            if signal in ('done', 'error'):
                break
        for filepath in paths:
            recorded = self._recorded_outcomes.setdefault(filepath, {'singleshot': set(), 'multishot': set()})
            self.record_outcomes(filepath, outcomes, recorded['multishot'])
            recorded['multishot'] = set(outcomes)
        self.fire_event('multishot_done', paths, outcomes)
        for filepath in paths:
            shot_outcomes = {**self._singleshot_outcomes.get(filepath, {}), **outcomes}
            if signal == 'done':
                del self._singleshot_outcomes[filepath]
                self.shots_model.end_attempt(filepath, outcome_state(shot_outcomes))
            else:
                self.shots_model.end_attempt(filepath, 'awaiting multishot')
            self.fire_event('analysis_done', filepath, shot_outcomes)
        if signal == 'done':
            self.multishot_required = False
            return True
        # Unless the routines after the failed one did not run, as when analysis is
        # paused, in which case all run on resuming:
        if status_percent is not None:
            self.multishot_required = False
        return False
