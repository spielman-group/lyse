#####################################################################
#                                                                   #
# /__main__.py                                                      #
#                                                                   #
# Copyright 2013, Monash University                                 #
#                                                                   #
# This file is part of the program lyse, in the labscript suite     #
# (see http://labscriptsuite.org), and is licensed under the        #
# Simplified BSD License. See the license.txt file in the root of   #
# the project for the full license.                                 #
#                                                                   #
#####################################################################
"""Top level Lyse GUI
"""
import os
import labscript_utils.excepthook
labscript_utils.excepthook.install()
import importlib.metadata

APPLICATION_NAME = 'lyse'
LYSE_DIR = os.path.dirname(__file__)

# Associate app windows with OS menu shortcuts, must be before any GUI calls, apparently
import desktop_app
desktop_app.set_process_appid(APPLICATION_NAME)

import labscript_utils.splash
splash = labscript_utils.splash.Splash(
    os.path.join(LYSE_DIR, 'lyse.svg'), application_name=APPLICATION_NAME
)
splash.show()

splash.update_text('importing standard library modules')
import sys
import time
import queue

splash.update_text('importing numpy')
import numpy as np
splash.update_text('importing pandas')
import pandas

splash.update_text('importing zprocess (zlog and zlock must be running)')
from labscript_utils.ls_zprocess import ProcessTree

splash.update_text('importing labscript suite modules')
from labscript_utils.labconfig import (
    LabConfig,
    load_appconfig,
)
from labscript_utils.setup_logging import setup_logging
from labscript_utils.qtwidgets.application import (
    LabscriptGuiApplication,
    exec_qapplication,
    setup_application,
)
from labscript_utils.qtwidgets.outputbox import OutputBox
from labscript_utils.qtwidgets.themed_window import FirstPaintWindow, ThemedWindow
import labscript_utils.qtwidgets.file_dialogs as file_dialogs

splash.update_text('importing qt modules')
from qtutils.qt import QtCore, QtWidgets, QT_ENV
from qtutils import UiLoader
QT_VERSION_STR = QtCore.qVersion()
PYQT_VERSION_STR = importlib.metadata.version(QT_ENV)

# Imported for its side effect: the qtutils icons that .ui files refer to resolve only once
# it is loaded.
import qtutils.icons

splash.update_text('importing core Lyse modules')
import lyse.utils
import lyse.routines
import lyse.filebox
import lyse.communication

class LyseMainWindow(ThemedWindow, FirstPaintWindow, QtWidgets.QMainWindow):
    def __init__(self, app, *args, **kwargs):
        self.app = app
        QtWidgets.QMainWindow.__init__(self, *args, **kwargs)
        self.closing = False

    def closeEvent(self, event):
        if self.closing:
            return QtWidgets.QMainWindow.closeEvent(self, event)
        if self.app.on_close_event():
            self.closing = True
            timeout_time = time.time() + 2
            self.delayedClose(timeout_time)
        event.ignore()

    def delayedClose(self, timeout_time):
        if not all(self.app.workers_terminated().values()) and time.time() < timeout_time:
            QtCore.QTimer.singleShot(50, lambda: self.delayedClose(timeout_time))
        else:
            QtCore.QTimer.singleShot(0, self.close)


class Lyse(LabscriptGuiApplication):

    app_name = 'lyse'
    default_config_filename = 'lyse.toml'
    config_description = 'which scripts are loaded and other GUI state'
    splitters = ('splitter', 'splitter_vertical', 'splitter_horizontal')

    def __init__(self, qapplication):
        self.logger = setup_logging(APPLICATION_NAME)
        labscript_utils.excepthook.set_logger(self.logger)
        self.logger.info('\n\n===============starting===============\n')
        self.logger.info('Qt Environment: %s', QT_ENV)
        self.logger.info('PySide/PyQt version: %s', PYQT_VERSION_STR)
        self.logger.info('Qt version: %s', QT_VERSION_STR)

        self.setup_config()

        self.process_tree = ProcessTree.instance()
        self.process_tree.zlock_client.set_process_name(APPLICATION_NAME)

        self.qapplication = qapplication
        loader = UiLoader()
        self.ui = loader.load(os.path.join(lyse.utils.LYSE_DIR, 'user_interface/main.ui'), LyseMainWindow(self))
        self.logger.info('UI loaded')
        self.init_config_window_title()

        self.connect_signals()

        # Queues connect each routinebox to the filebox.
        to_singleshot = queue.Queue()
        from_singleshot = queue.Queue()

        to_multishot = queue.Queue()
        from_multishot = queue.Queue()

        self.output_box = OutputBox(self.ui.verticalLayout_output_box)
        self.singleshot_routinebox = lyse.routines.RoutineBox(self, self.ui.verticalLayout_singleshot_routinebox, self.exp_config,
                                                self, to_singleshot, from_singleshot, self.output_box.port)
        self.multishot_routinebox = lyse.routines.RoutineBox(self, self.ui.verticalLayout_multishot_routinebox, self.exp_config,
                                               self, to_multishot, from_multishot, self.output_box.port, multishot=True)
        self.filebox = lyse.filebox.FileBox(self, self.ui.verticalLayout_filebox, self.exp_config,
                               to_singleshot, from_singleshot, to_multishot, from_multishot)
        self.logger.info('Boxes loaded')

        # Only now, since the server puts submitted shots in the filebox's queue. Until
        # then the port is unbound, and a client's request times out rather than failing.
        try:
            self.server = lyse.communication.LyseServer(self)
        except OSError as e:
            # Its port is taken, most likely by another copy of lyse.
            self.logger.error('%s', e)
            splash.hide()
            self.error_dialog(str(e))
            sys.exit(1)
        self.logger.info('Started lyse server on port %s', self.server.port)

        self.last_save_config_file = None
        self.last_save_data = None

        self.ui.actionLoad_configuration.triggered.connect(self.on_load_configuration_triggered)
        self.ui.actionRevert_configuration.triggered.connect(self.on_revert_configuration_triggered)
        self.ui.actionSave_configuration.triggered.connect(self.on_save_configuration_triggered)
        self.ui.actionSave_configuration_as.triggered.connect(self.on_save_configuration_as_triggered)
        self.ui.actionSave_dataframe_as.triggered.connect(lambda: self.on_save_dataframe_triggered(True))
        self.ui.actionSave_dataframe.triggered.connect(lambda: self.on_save_dataframe_triggered(False))
        self.ui.actionLoad_dataframe.triggered.connect(self.on_load_dataframe_triggered)
        self.ui.actionQuit.triggered.connect(self.ui.close)

        self.ui.resize(1600, 900)

        self.ui.splitter_horizontal.setSizes([1000, 600])
        self.ui.splitter_vertical.setSizes([300, 600])

        # autoload a config file, if labconfig is set to do so:
        active_config_file = self.get_default_config_file()
        try:
            autoload_config_file = self.exp_config.get('lyse', 'autoload_config_file')
        except (LabConfig.NoOptionError, LabConfig.NoSectionError):
            self.output_box.output('Ready.\n\n')
        else:
            active_config_file = autoload_config_file
            self.ui.setEnabled(False)
            self.output_box.output(f'Loading default config file {autoload_config_file}...')

            def load_the_config_file():
                try:
                    self.load_configuration(autoload_config_file, restore_window_geometry)
                    self.output_box.output('done.\n')
                except Exception as e:
                    self.output_box.output(
                        f'\nCould not load config file: {e.__class__.__name__}: {e}\n\n', red=True)
                else:
                    self.output_box.output('Ready.\n\n')
                finally:
                    self.ui.setEnabled(True)
            # Load the window geometry now, but then defer the other loading until 50ms
            # after the window has shown, so that the GUI pops up faster in the meantime.
            try:
                self.load_window_geometry_configuration(autoload_config_file)
            except Exception:
                # ignore error for now and let it be raised again in the call to load_configuration:
                restore_window_geometry = True
            else:
                # Success - skip loading window geometry in load_configuration:
                restore_window_geometry = False
            self.ui.firstPaint.connect(lambda: QtCore.QTimer.singleShot(50, load_the_config_file))
        self.logger.info('lyse configuration loaded')

        self.set_config_window_title(active_config_file)
        self.ui.show()

    def terminate_all_workers(self):
        for routine in self.singleshot_routinebox.routines + self.multishot_routinebox.routines:
            routine.end_child()

    def workers_terminated(self):
        terminated = {}
        for routine in self.singleshot_routinebox.routines + self.multishot_routinebox.routines:
            routine.worker.poll()
            terminated[routine.filepath] = routine.worker.returncode is not None
        return terminated

    def on_close_event(self):
        if not self.offer_to_save_changes('Quit lyse', quitting=True):
            return False
        self.terminate_all_workers()
        return True

    def get_save_data(self):
        save_data = {}

        box = self.singleshot_routinebox
        save_data['singleshot'] = list(zip([routine.filepath for routine in box.routines],
                                           [box.model.item(row, box.COL_ACTIVE).checkState().value
                                            for row in range(box.model.rowCount())]))
        save_data['lastsingleshotfolder'] = box.last_opened_routine_folder
        box = self.multishot_routinebox
        save_data['multishot'] = list(zip([routine.filepath for routine in box.routines],
                                          [box.model.item(row, box.COL_ACTIVE).checkState().value
                                           for row in range(box.model.rowCount())]))
        save_data['lastmultishotfolder'] = box.last_opened_routine_folder

        save_data['lastfileboxfolder'] = self.filebox.last_opened_shots_folder

        save_data['analysis_paused'] = self.filebox.analysis_paused
        save_data.update(self.geometry_save_data())
        return save_data

    def load_configuration(self, filename, restore_window_geometry=True):
        # LEGACY INI COMPATIBILITY. DEPRECATED CODE, WILL BE REMOVED.
        appconfig, save_target = load_appconfig(filename, return_save_path=True)
        save_data = appconfig.get('lyse_state', {})
        lyse.routines.add_routines([
            (box, save_data[name], True)
            for box, name in [(self.singleshot_routinebox, 'singleshot'),
                              (self.multishot_routinebox, 'multishot')]
            if name in save_data
        ])
        if 'lastsingleshotfolder' in save_data:
            self.singleshot_routinebox.last_opened_routine_folder = save_data['lastsingleshotfolder']
        if 'lastmultishotfolder' in save_data:
            self.multishot_routinebox.last_opened_routine_folder = save_data['lastmultishotfolder']
        if 'lastfileboxfolder' in save_data:
            self.filebox.last_opened_shots_folder = save_data['lastfileboxfolder']
        if 'analysis_paused' in save_data and save_data['analysis_paused']:
            self.filebox.set_analysis_paused(True)
        if restore_window_geometry:
            self.restore_geometry(save_data)
        self.configuration_loaded(save_target)

    def load_window_geometry_configuration(self, filename):
        """Load only the window geometry, so it can be applied before the window is shown."""
        # LEGACY INI COMPATIBILITY. DEPRECATED CODE, WILL BE REMOVED.
        self.restore_geometry(load_appconfig(filename).get('lyse_state', {}))

    def setup_config(self):
        required_config_params = {"default": ["apparatus_name"],
                                  "paths": ["shared_drive",
                                            "experiment_shot_storage",
                                            "analysislib"],
                                  "ports": ["lyse"]
                                  }
        self.exp_config = LabConfig(required_params=required_config_params)

    def connect_signals(self):
        QtWidgets.QShortcut('Del', self.ui, lambda: self.delete_items(True))
        QtWidgets.QShortcut('Shift+Del', self.ui, lambda: self.delete_items(False))

    def on_save_dataframe_triggered(self, choose_folder=True):
        folder = None
        if choose_folder:
            default = self.exp_config.get('paths', 'experiment_shot_storage')
            folder = file_dialogs.choose_folder(self.ui, 'Select a Folder for the Dataframes', default)
            if not folder:
                # User cancelled
                return
        try:
            self.save_dataframe(folder)
        except ValueError as e:
            self.error_dialog(str(e))

    def save_dataframe(self, folder=None):
        """Save the dataframe, one file per sequence, in folder, or else beside each
        sequence's shots, and return the files written."""
        df = self.filebox.shots_model.dataframe.copy()
        if len(df) == 0:
            raise ValueError('The dataframe is empty')
        saved = []
        # A sequence is its engage time and labscript: two labscripts
        # engaged in the same second are two sequences, saved apart.
        for sequence, labscript in dict.fromkeys(zip(df['sequence'], df['labscript'])):
            rows = (df['sequence'] == sequence) & (df['labscript'] == labscript)
            sequence_df = pandas.DataFrame(df[rows], columns=df.columns).dropna(axis=1, how='all')
            filename = f'dataframe_{sequence.to_pydatetime().strftime("%Y%m%dT%H%M%S")}_{labscript[:-3]}.pkl'
            save_path = folder or os.path.dirname(sequence_df['filepath'].iloc[0])
            for col in sequence_df.columns :
                if sequence_df[col].dtype == object:
                    # Convert only all-numeric columns: image attributes
                    # arrive as bytes (ValueError), and array-valued
                    # results cannot be converted at all (TypeError).
                    try:
                        sequence_df[col] = pandas.to_numeric(sequence_df[col])
                    except (ValueError, TypeError):
                        pass
            sequence_df = sequence_df.infer_objects()
            sequence_df.to_pickle(os.path.join(save_path, filename))
            saved.append(os.path.join(save_path, filename))
        return saved

    def on_load_dataframe_triggered(self):
        default = os.path.join(self.exp_config.get('paths', 'experiment_shot_storage'), 'dataframe.pkl')
        file = file_dialogs.open_file(
            self.ui,
            'Select dataframe file to load',
            default,
            "dataframe files (*.pkl)",
        )
        if not file:
            # User cancelled
            return
        self.load_dataframe(file)

    def load_dataframe(self, file):
        """Add the shots of a saved dataframe, reading again those changed since it was saved."""
        df = pandas.read_pickle(file).sort_values("run time").reset_index(drop=True)

        # Check for changes in the shot files since the dataframe was exported
        def changed_since(filepath, time):
            if os.path.isfile(filepath):
                return os.path.getmtime(filepath) > time
            else:
                return False

        filepaths = df["filepath"].tolist()
        changetime_cache = os.path.getmtime(file)
        need_updating = np.where(list(map(lambda x: changed_since(x, changetime_cache), filepaths)))[0]
        need_updating = np.sort(need_updating)[::-1]  # sort in descending order to not remove the wrong items with pop

        # Reload the files that changed since exporting
        for index in need_updating:
            filepath = filepaths.pop(index)
            self.filebox.incoming_queue.put((filepath, 'dataframe'))
        df = df.drop(need_updating)

        self.filebox.shots_model.add_files(filepaths, df, done=True)

    def delete_items(self, confirm):
        """Delete items from whichever box has focus, optionally confirming first."""
        if self.filebox.ui.tableView.hasFocus():
            self.filebox.shots_model.remove_selection(confirm)
        if self.singleshot_routinebox.ui.treeView.hasFocus():
            self.singleshot_routinebox.remove_selection(confirm)
        if self.multishot_routinebox.ui.treeView.hasFocus():
            self.multishot_routinebox.remove_selection(confirm)


if __name__ == "__main__":

    splash.update_text('starting GUI')
    qapplication = labscript_utils.splash.get_qapplication()

    extra_styles = """
    QTreeView:item:selected { color: palette(highlighted-text); }

    QTreeView:item:hover { color: palette(highlighted-text); }

    QTableView:item:selected { color: palette(highlighted-text); }

    QTableView:item:hover { color: palette(highlighted-text); }
    """

    current_style = qapplication.styleSheet()
    qapplication.setStyleSheet(current_style + extra_styles)

    setup_application(qapplication)
    app = Lyse(qapplication)
    # Before exec_qapplication deletes the windows, one of which the output box writes to:
    qapplication.aboutToQuit.connect(app.output_box.shutdown)

    splash.hide()
    exec_qapplication(qapplication)

    app.server.shutdown()
