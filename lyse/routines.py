#####################################################################
#                                                                   #
# /routines.py                                                      #
#                                                                   #
# Copyright 2013, Monash University                                 #
#                                                                   #
# This file is part of the program lyse, in the labscript suite     #
# (see http://labscriptsuite.org), and is licensed under the        #
# Simplified BSD License. See the license.txt file in the root of   #
# the project for the full license.                                 #
#                                                                   #
#####################################################################

"""
analysis GUI code
"""

import contextlib
import os
import sys
import time
import types
import logging
import threading
from concurrent.futures import ThreadPoolExecutor

# Labscript imports
import zprocess
from labscript_utils.programs import open_file
from labscript_utils.qtwidgets.headerview_with_widgets import HorizontalHeaderViewWithWidgets
from labscript_utils.qtwidgets.leftclick import TreeView
import labscript_utils.qtwidgets.file_dialogs as file_dialogs
from zprocess import ProcessExited

# qt imports
from qtutils.qt import QtCore, QtGui, QtWidgets
from qtutils import inmain_decorator, UiLoader, DisconnectContextManager
from qtutils.icons import Icon

import lyse.widgets
import lyse.utils
import lyse.utils.worker

class RoutineBox:

    COL_ACTIVE = 0
    COL_STATUS = 1
    COL_NAME = 2
    ROLE_FULLPATH = QtCore.Qt.UserRole + 1

    def __init__(self, app, container, exp_config, filebox, from_filebox, to_filebox, output_box_port, multishot=False):
        self.app = app
        self.multishot = multishot
        self.box_name = 'multishot' if multishot else 'singleshot'
        self.filebox = filebox
        self.exp_config = exp_config
        self.from_filebox = from_filebox
        self.to_filebox = to_filebox
        self.output_box_port = output_box_port

        self.logger = logging.getLogger(f'lyse.RoutineBox.{self.box_name}')
        self.logger.info('starting')

        loader = UiLoader()
        loader.registerCustomWidget(TreeView)
        self.ui = loader.load(os.path.join(lyse.utils.LYSE_DIR, 'user_interface/routinebox.ui'))
        container.addWidget(self.ui)
        self.logger.info('UI loaded')

        if multishot:
            self.ui.groupBox.setTitle('Multishot routines')
        else:
            self.ui.groupBox.setTitle('Singleshot routines')
            self.ui.toolButton_run_routines.hide()

        self.model = QtGui.QStandardItemModel()
        self.header = HorizontalHeaderViewWithWidgets(self.model)
        self.ui.treeView.setHeader(self.header)
        self.ui.treeView.setModel(self.model)
        self.ui.treeView.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)

        active_item = QtGui.QStandardItem()
        active_item.setToolTip('Whether the analysis routine should run')
        status_item = QtGui.QStandardItem()
        status_item.setIcon(Icon(':qtutils/fugue/information'))
        status_item.setToolTip('The status of this analyis routine\'s execution')
        name_item = QtGui.QStandardItem('name')
        name_item.setToolTip('The name of the python script for the analysis routine')

        self.select_all_checkbox = QtWidgets.QCheckBox()
        self.select_all_checkbox.setToolTip('whether the analysis routine should run')
        self.header.setWidget(self.COL_ACTIVE, self.select_all_checkbox)
        self.header.setStretchLastSection(True)
        self.select_all_checkbox.setTristate(False)

        self.model.setHorizontalHeaderItem(self.COL_ACTIVE, active_item)
        self.model.setHorizontalHeaderItem(self.COL_STATUS, status_item)
        self.model.setHorizontalHeaderItem(self.COL_NAME, name_item)

        self.header.setMinimumSectionSize(0)
        self.ui.treeView.resizeColumnToContents(self.COL_ACTIVE)
        self.ui.treeView.resizeColumnToContents(self.COL_STATUS)

        self.ui.treeView.setContextMenuPolicy(QtCore.Qt.CustomContextMenu)
        self.action_set_selected_active = QtWidgets.QAction(
            Icon(':qtutils/fugue/ui-check-box'), 'set selected routines active',  self.ui)
        self.action_set_selected_inactive = QtWidgets.QAction(
            Icon(':qtutils/fugue/ui-check-box-uncheck'), 'set selected routines inactive',  self.ui)
        self.action_restart_selected = QtWidgets.QAction(
            Icon(':qtutils/fugue/arrow-circle'), 'restart worker process for selected routines',  self.ui)
        self.action_show_windows_selected = QtWidgets.QAction(
            Icon(':qtutils/fugue/applications-blue'), 'show windows for selected routines',  self.ui)
        self.action_remove_selected = QtWidgets.QAction(
            Icon(':qtutils/fugue/minus'), 'Remove selected routines',  self.ui)

        self.action_add_routines = QtWidgets.QAction(
            Icon(':/qtutils/fugue/script--plus'), 'Add classic scripts (.py)…', self.ui)
        self.action_add_routine_folder = QtWidgets.QAction(
            Icon(':/qtutils/fugue/folder--plus'), 'Add a GUI routine folder (.lyse)…', self.ui)
        add_menu = QtWidgets.QMenu(self.ui.toolButton_add_routines)
        add_menu.addActions([self.action_add_routines, self.action_add_routine_folder])
        self.ui.toolButton_add_routines.setMenu(add_menu)
        self.last_opened_routine_folder = self.exp_config.get('paths', 'analysislib')

        self.routines = []

        self.connect_signals()

        self.logger.info('starting analysis loop')
        self.analysis = threading.Thread(target = self.analysis_loop)
        self.analysis.daemon = True
        self.analysis.start()

    def connect_signals(self):
        self.action_add_routines.triggered.connect(self.on_add_routines_clicked)
        self.action_add_routine_folder.triggered.connect(self.on_add_routine_folder_clicked)
        self.ui.toolButton_remove_routines.clicked.connect(self.on_remove_selection)
        self.model.itemChanged.connect(self.on_model_item_changed)
        self.ui.treeView.doubleLeftClicked.connect(self.on_treeview_double_left_clicked)
        self.model_item_changed_disconnected = DisconnectContextManager(
            self.model.itemChanged, self.on_model_item_changed)
        self.select_all_checkbox.stateChanged.connect(self.on_select_all_state_changed)
        self.select_all_checkbox_state_changed_disconnected = DisconnectContextManager(
            self.select_all_checkbox.stateChanged, self.on_select_all_state_changed)
        self.ui.treeView.customContextMenuRequested.connect(self.on_treeView_context_menu_requested)
        self.action_set_selected_active.triggered.connect(
            lambda: self.set_routines_active(self.selected_filepaths(), True))
        self.action_set_selected_inactive.triggered.connect(
            lambda: self.set_routines_active(self.selected_filepaths(), False))
        self.action_restart_selected.triggered.connect(
            lambda: self.restart_routines(self.selected_filepaths()))
        self.action_show_windows_selected.triggered.connect(self.on_show_windows_triggered)
        self.action_remove_selected.triggered.connect(self.on_remove_selection)
        tree = self.ui.treeView
        self.ui.toolButton_move_to_top.clicked.connect(lambda: tree.move_selected_rows('top'))
        self.ui.toolButton_move_up.clicked.connect(lambda: tree.move_selected_rows('up'))
        self.ui.toolButton_move_down.clicked.connect(lambda: tree.move_selected_rows('down'))
        self.ui.toolButton_move_to_bottom.clicked.connect(
            lambda: tree.move_selected_rows('bottom')
        )
        tree.rowsReordered.connect(self.on_rows_reordered)

    def on_add_routines_clicked(self):
        routine_files = file_dialogs.open_files(
            self.ui,
            'Select analysis routines',
            self.last_opened_routine_folder,
            "Python scripts (*.py)",
        )
        if not routine_files:
            # User cancelled selection
            return

        # Save the containing folder for use next time we open the dialog box:
        self.last_opened_routine_folder = os.path.dirname(routine_files[0])
        add_routines([(self, [(routine_file, QtCore.Qt.Checked) for routine_file in routine_files], False)])

    def on_add_routine_folder_clicked(self):
        folder = file_dialogs.choose_folder(
            self.ui, 'Select a GUI analysis routine folder', self.last_opened_routine_folder)
        if not folder:
            return
        self.last_opened_routine_folder = os.path.dirname(folder)
        if os.path.splitext(folder)[1] != lyse.utils.GUI_ROUTINE_SUFFIX:
            self.app.output_box.output(
                f'Warning: Ignoring {folder}: a GUI routine folder is named <name>{lyse.utils.GUI_ROUTINE_SUFFIX}\n',
                red=True)
            return
        add_routines([(self, [(folder, QtCore.Qt.CheckState.Checked)], False)])

    def on_treeview_double_left_clicked(self, index):
        if index.column() != self.COL_NAME:
            return
        name_item = self.model.item(index.row(), self.COL_NAME)
        routine_filepath = name_item.data(self.ROLE_FULLPATH)
        if os.path.splitext(routine_filepath)[1] == lyse.utils.GUI_ROUTINE_SUFFIX:
            routine_filepath = os.path.join(routine_filepath, 'lyse_routine.py')
        try:
            open_file(routine_filepath, 'text_editor', self.exp_config)
        except Exception as e:
            self.app.error_dialog("Unable to launch text editor. Error was: %s. The text editor is set in the [programs] section of %s." %
                (str(e), self.exp_config.config_path))
                         
    def on_remove_selection(self):
        self.remove_selection()

    def remove_selection(self, confirm=True):
        filepaths = self.selected_filepaths()
        if not filepaths:
            return
        if confirm and not self.app.question_dialog(f"Remove {len(filepaths)} routines?"):
            return
        self.remove_routines(filepaths)

    def selected_filepaths(self):
        rows = sorted({index.row() for index in self.ui.treeView.selectedIndexes()})
        return [self.model.item(row, self.COL_NAME).data(self.ROLE_FULLPATH) for row in rows]

    def _routines_named(self, filepaths):
        """The routines with these file paths, in the box's order. Raise ValueError
        naming any path that is not a routine of the box."""
        unknown = set(filepaths) - {routine.filepath for routine in self.routines}
        if unknown:
            raise ValueError(f'Not a routine of the {self.box_name} box: {", ".join(sorted(unknown))}')
        return [routine for routine in self.routines if routine.filepath in filepaths]

    def remove_routines(self, filepaths):
        for routine in self._routines_named(filepaths):
            routine.remove()
            self.routines.remove(routine)
            self.logger.info('removing routine for %s', routine.filepath)
        self.update_select_all_checkstate()

    def set_routines_active(self, filepaths, active):
        state = QtCore.Qt.CheckState.Checked if active else QtCore.Qt.CheckState.Unchecked
        for routine in self._routines_named(filepaths):
            self.model.item(routine.get_row_index(), self.COL_ACTIVE).setCheckState(state)
        self.update_select_all_checkstate()

    def reorder_routines(self, filepaths):
        """Put the box's routines in the order of filepaths, which names each once."""
        if sorted(filepaths) != sorted(routine.filepath for routine in self.routines):
            raise ValueError(f'The new order must name each routine of the {self.box_name} box once')
        self.ui.treeView.reorder_rows([filepaths.index(routine.filepath) for routine in self.routines])

    def restart_routines(self, filepaths):
        for routine in self._routines_named(filepaths):
            routine.restart()

    def on_model_item_changed(self, item):
        if item.column() == self.COL_ACTIVE:
            self.update_select_all_checkstate()

    def on_select_all_state_changed(self, state):
        with self.select_all_checkbox_state_changed_disconnected:
            # Do not allow a switch *to* a partially checked state:
            self.select_all_checkbox.setTristate(False)
        state = self.select_all_checkbox.checkState()
        with self.model_item_changed_disconnected:
            for row in range(self.model.rowCount()):
                active_item = self.model.item(row, self.COL_ACTIVE)
                active_item.setCheckState(state)

    def on_treeView_context_menu_requested(self, point):
        menu = QtWidgets.QMenu(self.ui.treeView)
        menu.addAction(self.action_set_selected_active)
        menu.addAction(self.action_set_selected_inactive)
        menu.addAction(self.action_restart_selected)
        menu.addAction(self.action_show_windows_selected)
        menu.addAction(self.action_remove_selected)
        menu.exec(QtGui.QCursor.pos())

    def on_show_windows_triggered(self):
        for routine in self._routines_named(self.selected_filepaths()):
            routine.show_windows()
        self.update_select_all_checkstate()

    def analysis_loop(self):
        while True:
            filepath = self.from_filebox.get()
            paths = None
            if self.multishot:
                # A multishot routine box is given the shot files analysed
                # since its last pass, rather than a shot file:
                filepath, paths = None, filepath
            self.logger.info('got a file to process: %s', filepath)
            self.do_analysis(filepath, paths)
    
    def _start_next_routine(self):
        """Set the status of the first enabled routine that has not run to working, and
        return it. Return None if there is no such routine. Call from the main
        thread."""
        for routine in self.routines:
            if routine.enabled() and not routine.done and not routine.error:
                routine.set_status('working')
                return routine
        return None

    @inmain_decorator()
    def start_analysis(self):
        """Clear the status of every routine and start the first enabled one. Return
        it, or None if no routine is enabled."""
        for routine in self.routines:
            routine.set_status('clear')
        return self._start_next_routine()

    @inmain_decorator()
    def end_routine(self, routine, success):
        """Set the status of a routine that has run, and start the next enabled routine
        that has not run. After a failure, start it only in the multishot routine box,
        and only if analysis is not paused. Return the next routine, or None, and the
        percentage of enabled routines that have run, or (None, None) if the routines
        after the failed one are not run."""
        if success:
            routine.set_status('done')
        else:
            routine.set_status('error')
            if not self.multishot or self.app.filebox.analysis_paused:
                return None, None
        enabled_routines = [r for r in self.routines if r.enabled()]
        if enabled_routines:
            n_run = len([r for r in enabled_routines if r.done or r.error])
            status_percent = 100 * n_run / len(enabled_routines)
        else:
            # All routines got deleted mid-analysis, we're done here:
            status_percent = 100.0
        return self._start_next_routine(), status_percent

    def do_analysis(self, filepath, paths):
        """Run all analysis routines once, on filepath (singleshot box) or, for a
        multishot box, on paths: the shot files since its last pass (lyse.paths)."""
        error = stopped = False
        routine = self.start_analysis()
        while routine is not None:
            self.logger.info('running analysis routine %s', routine.shortname)
            success, updated_data = routine.do_analysis(filepath, paths)
            routine, status_percent = self.end_routine(routine, success)
            if success:
                self.logger.debug('success')
            else:
                self.logger.debug('failure')
                error = True
                stopped = status_percent is None
            self.to_filebox.put(['progress', status_percent, updated_data])
        # 'error' if a routine failed, so that the filebox offers the pass's shots
        # again, with no status if the routines after it did not run:
        if error:
            self.to_filebox.put(['error', None if stopped else 100.0, {}])
        else:
            self.to_filebox.put(['done', 100.0, {}])
        self.logger.debug('completed analysis of %s', filepath)

    def abort(self):
        """Interrupt the routine that is running, if any."""
        for routine in self.routines:
            routine.abort()

    def on_rows_reordered(self, order):
        self.routines = [self.routines[order.index(i)] for i in range(len(order))]

    def update_select_all_checkstate(self):
        with self.select_all_checkbox_state_changed_disconnected:
            all_states = []
            for row in range(self.model.rowCount()):
                active_item = self.model.item(row, self.COL_ACTIVE)
                all_states.append(active_item.checkState())
            if all(state == QtCore.Qt.Checked for state in all_states):
                self.select_all_checkbox.setCheckState(QtCore.Qt.Checked)
            elif all(state == QtCore.Qt.Unchecked for state in all_states):
                self.select_all_checkbox.setCheckState(QtCore.Qt.Unchecked)
            else:
                self.select_all_checkbox.setCheckState(QtCore.Qt.PartiallyChecked)


def add_routines(additions):
    """Add routines to routine boxes, starting all their new workers together.

    additions holds (routinebox, routine_files, clear_existing) tuples, with
    routine_files (filepath, checked) tuples. With clear_existing, the box's
    routines not in routine_files are removed and those in it restarted, so the
    box holds routine_files in order."""
    new = []
    orders = []
    for box, routine_files, clear_existing in additions:
        existing = {routine.filepath: routine for routine in box.routines}
        if clear_existing:
            filepaths = [filepath for filepath, checked in routine_files]
            for routine in box.routines[:]:
                if routine.filepath not in filepaths:
                    routine.remove()
                    box.routines.remove(routine)
        added = []
        for filepath, checked in routine_files:
            if filepath in added or (not clear_existing and filepath in existing):
                box.app.output_box.output(f'Warning: Ignoring duplicate analysis routine {filepath}\n', red=True)
                continue
            added.append(filepath)
            if clear_existing and filepath in existing:
                # A restart, unlike a new routine, ends the old worker before the new one starts.
                routine = existing[filepath]
                routine.restart()
                routine.set_status('clear')
                active_item = box.model.item(routine.get_row_index(), box.COL_ACTIVE)
                active_item.setCheckState(QtCore.Qt.CheckState(checked))
            else:
                new.append((box, filepath, QtCore.Qt.CheckState(checked)))
        if clear_existing:
            orders.append((box, added))

    # Each start waits for its worker to connect, so they wait together.
    starts = []
    if new:
        with ThreadPoolExecutor(max_workers=len(new)) as executor:
            starts = [executor.submit(start_worker, box.app.process_tree, filepath,
                                      box.output_box_port, checked == QtCore.Qt.Checked)
                      for box, filepath, checked in new]
    for (box, filepath, checked), started in zip(new, starts):
        try:
            worker = started.result()
        except Exception:
            # The other routines are still added.
            zprocess.raise_exception_in_thread(sys.exc_info())
            continue
        box.logger.info('adding routine for %s', filepath)
        box.routines.append(
            AnalysisRoutine(box.app, filepath, box.model, box.output_box_port, checked, worker))
    for box, added in orders:
        # Less any whose worker failed to start:
        started = {routine.filepath for routine in box.routines}
        box.reorder_routines([filepath for filepath in added if filepath in started])
    for box, _, _ in additions:
        box.update_select_all_checkstate()


def start_worker(process_tree, filepath, output_box_port, active):
    """Start the worker of the routine at filepath, and return the queues to and from
    it and its process, once it has connected."""
    logger = logging.getLogger(f'lyse.AnalysisRoutine.{os.path.basename(filepath)}')
    logger.info('starting worker')
    to_worker, from_worker, worker = process_tree.subprocess(
        os.path.join(lyse.utils.LYSE_DIR, 'analysis_subprocess.py'),
        output_redirection_port=output_box_port,
        startup_timeout=30,
    )
    # Tell the worker what script it will be executing, and whether the routine is active:
    to_worker.put((filepath, active))
    logger.info('analysis_subprocess started')
    return to_worker, from_worker, worker


class AnalysisRoutine:

    # Seconds for which end_child() waits for the worker to exit before killing it. The
    # worker exits within _ABORT_TIMEOUT + _KILL_LOCK_TIMEOUT unless it is unresponsive.
    QUIT_TIMEOUT = lyse.utils.worker._ABORT_TIMEOUT + lyse.utils.worker._KILL_LOCK_TIMEOUT + 5

    def __init__(self, app, filepath, model, output_box_port, checked, worker):
        self.app = app
        self.filepath = filepath
        self.shortname = os.path.basename(self.filepath)
        self.model = model
        self.output_box_port = output_box_port

        self.COL_ACTIVE = RoutineBox.COL_ACTIVE
        self.COL_STATUS = RoutineBox.COL_STATUS
        self.COL_NAME = RoutineBox.COL_NAME
        self.ROLE_FULLPATH = RoutineBox.ROLE_FULLPATH

        self.error = False
        self.done = False
        self.working = False
        # Counts the runs, so that abort() can tell whether the one it interrupted goes on:
        self.run_number = 0

        self.logger = logging.getLogger(f'lyse.AnalysisRoutine.{self.shortname}')

        # As start_worker returns them:
        self.to_worker, self.from_worker, self.worker = worker

        active_item =  QtGui.QStandardItem()
        active_item.setCheckable(True)
        active_item.setCheckState(checked)
        info_item = QtGui.QStandardItem()
        name_item = QtGui.QStandardItem(self.shortname)
        name_item.setToolTip(self.filepath)
        name_item.setData(self.filepath, self.ROLE_FULLPATH)
        self.model.appendRow([active_item, info_item, name_item])

        self.shutdown = None

    @inmain_decorator()
    def send_analysis(self, filepath, paths):
        # On the GUI thread, where shutdowns start, so that none can start between the
        # check and the send. Returns whether one is pending, and the reply's queue if sent.
        if self.shutdown is not None:
            return True, None
        # A shot put to a worker that has just exited can still be taken by its socket and
        # lost, and the reply then never comes.
        if self.worker.poll() is not None:
            return False, None
        try:
            self.to_worker.put(['analyse', (filepath, paths)])
        except ProcessExited:
            return False, None
        return False, self.from_worker

    def do_analysis(self, filepath, paths):
        # Wait out a pending shutdown, rather than send the shot to a worker that is quitting.
        pending, from_worker = self.send_analysis(filepath, paths)
        while pending:
            time.sleep(0.05)
            pending, from_worker = self.send_analysis(filepath, paths)
        if from_worker is None:
            # Removed, or its worker died: nothing would answer.
            return False, {}
        signal, data = from_worker.get()
        if signal == 'error':
            return False, data
        elif signal == 'done':
            return True, data
        else:
            raise ValueError(f'invalid signal {signal}')

    @inmain_decorator()
    def set_status(self, status):
        self.working = status == 'working'
        if self.working:
            self.run_number += 1
        index = self.get_row_index()
        if index is None:
            # Yelp, we've just been deleted. Nothing to do here.
            return
        status_item = self.model.item(index, self.COL_STATUS)
        if status == 'done':
            status_item.setIcon(Icon(':/qtutils/fugue/tick'))
            self.done = True
            self.error = False
        elif status == 'working':
            status_item.setIcon(Icon(':/qtutils/fugue/hourglass'))
            self.done = False
            self.error = False
        elif status == 'error':
            status_item.setIcon(Icon(':/qtutils/fugue/exclamation'))
            self.error = True
            self.done = False
        elif status == 'clear':
            status_item.setData(None, QtCore.Qt.DecorationRole)
            self.done = False
            self.error = False
        else:
            raise ValueError(status)

    @inmain_decorator()
    def enabled(self):
        index = self.get_row_index()
        if index is None:
            # Yelp, we've just been deleted.
            return False
        enabled_item = self.model.item(index, self.COL_ACTIVE)
        return (enabled_item.checkState() == QtCore.Qt.Checked)

    def get_row_index(self):
        """Row index of this routine in the model, or None once it is removed."""
        for row in range(self.model.rowCount()):
            name_item = self.model.item(row, self.COL_NAME)
            fullpath = name_item.data(self.ROLE_FULLPATH)
            if fullpath == self.filepath:
                return row

    def abort(self):
        """Interrupt the routine if it is running. If it has not stopped after
        _ABORT_TIMEOUT seconds, restart its worker."""
        if not self.working or self.shutdown is not None:
            return
        # A worker that does not take the abort is replaced below.
        with contextlib.suppress(ProcessExited, zprocess.TimeoutError):
            self.to_worker.put(['abort', None], timeout=1)
        run_number = self.run_number
        QtCore.QTimer.singleShot(lyse.utils.worker._ABORT_TIMEOUT * 1000,
                                 lambda: self.check_aborted(run_number))

    def check_aborted(self, run_number):
        if self.working and self.run_number == run_number and self.shutdown is None:
            self.app.output_box.output(f'{self.shortname} did not stop, restarting its worker\n', red=True)
            self.restart()

    def restart(self):
        self.end_child(restart=True)

    @inmain_decorator()
    def show_windows(self):
        # On the GUI thread, where shutdowns start.
        if self.shutdown is None:
            with contextlib.suppress(ProcessExited):
                self.to_worker.put(['show', None])

    def remove(self):
        """End the child process and remove from the treeview"""
        self.end_child()
        index = self.get_row_index()
        if index is None:
            # Already gone
            return
        self.model.removeRow(index)

    def end_child(self, restart=False):
        """Tell the worker to quit, and kill it if it has not exited after QUIT_TIMEOUT
        seconds. The worker interrupts a running routine, and if that has not stopped
        after _ABORT_TIMEOUT seconds, exits once it has no HDF5 file open, or after
        _KILL_LOCK_TIMEOUT seconds."""
        if self.shutdown is not None:
            # One shutdown at a time: a removal or quit cancels a pending restart,
            # and a restart changes nothing.
            if not restart:
                self.shutdown.restart = False
            return
        # A worker that has exited never takes the quit.
        with contextlib.suppress(ProcessExited):
            self.to_worker.put(['quit', None])
        timeout_time = time.time() + self.QUIT_TIMEOUT
        self.shutdown = types.SimpleNamespace(
            worker=self.worker, from_worker=self.from_worker, restart=restart)
        QtCore.QTimer.singleShot(50, lambda: self.check_child_exited(timeout_time))

    def check_child_exited(self, timeout_time):
        shutdown = self.shutdown
        worker = shutdown.worker
        worker.poll()
        # timeout_time is None once kill() has been sent: only wait for the process to go.
        if worker.returncode is None and (timeout_time is None or time.time() < timeout_time):
            QtCore.QTimer.singleShot(50, lambda: self.check_child_exited(timeout_time))
            return
        elif worker.returncode is None:
            worker.kill()
            self.app.output_box.output(f'{self.shortname} worker killed\n', red=True)
            QtCore.QTimer.singleShot(50, lambda: self.check_child_exited(None))
            return
        elif timeout_time is None:
            # killed: the message was printed when kill() was sent.
            pass
        elif worker.returncode != 0:
            self.app.output_box.output(
                f'{self.shortname} worker exited with return code {worker.returncode}\n', red=True)
        else:
            self.app.output_box.output(f'{self.shortname} worker exited cleanly\n')

        # if analysis was running notify analysisloop that analysis has failed
        shutdown.from_worker.put(('error', {}))
        self.shutdown = None

        if shutdown.restart:
            self.to_worker, self.from_worker, self.worker = start_worker(
                self.app.process_tree, self.filepath, self.output_box_port, self.enabled())
            self.app.output_box.output(f'{self.shortname} worker restarted\n')
