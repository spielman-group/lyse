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

# Associate app windows with OS menu shortcuts, must be before any GUI calls, apparently
import desktop_app
desktop_app.set_process_appid('lyse')

# Splash screen
import labscript_utils.splash
splash = labscript_utils.splash.Splash(os.path.join(os.path.dirname(__file__), 'lyse.svg'))
splash.show()

splash.update_text('importing standard library modules')
import time
import sys
import queue
import warnings

# 3rd party imports:
splash.update_text('importing numpy')
import numpy as np
splash.update_text('importing pandas')
import pandas

# Labscript imports
splash.update_text('importing zprocess (zlog and zlock must be running)')
from labscript_utils.ls_zprocess import ProcessTree

splash.update_text('importing labscript suite modules')
from labscript_utils.labconfig import (
    LabConfig,
    get_app_saved_configs_dir,
    save_appconfig,
    load_appconfig,
)
from labscript_utils.setup_logging import setup_logging
from labscript_utils.qtwidgets.appconfig import (
    AppConfigActions,
    error_dialog,
    select_directory,
    select_open_file,
)
from labscript_utils.qtwidgets.outputbox import OutputBox
from labscript_utils import dedent
from labscript_utils.splash import FirstPaintMainWindow

# qt imports
splash.update_text('importing qt modules')
from qtutils.qt import QtCore, QtWidgets
from qtutils import UiLoader

# needs to be present so that qtutils icons referenced in .ui files can be resolved.  Since this is 
# magical is should not be implemented in this way.
import qtutils.icons 

# Lyse imports
splash.update_text('importing core Lyse modules')
import lyse.utils
import lyse.utils.gui
import lyse.routines
import lyse.filebox
import lyse.communication

class LyseMainWindow(FirstPaintMainWindow):
    def __init__(self, app, *args, **kwargs):
        self.app = app
        super().__init__(*args, **kwargs)
        self.closing = False

    def closeEvent(self, event):
        if self.closing:
            return super().closeEvent(event)
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

class Lyse(object):

    def __init__(self, qapplication):
        # First: Start logging
        self.logger = setup_logging('lyse')
        labscript_utils.excepthook.set_logger(self.logger)
        self.logger.info('\n\n===============starting===============\n')

        # Second: read lyse config
        self.setup_config()

        # Third: connect to zprocess and set a meaningful name for zlock client id:
        self.process_tree = ProcessTree.instance()
        self.process_tree.zlock_client.set_process_name('lyse')

        # Forth: start remote communication server
        self.port = int(self.exp_config.get('ports', 'lyse'))
        self.server = lyse.communication.WebServer(self,  self.port)

        # Last: UI setup
        self.qapplication = qapplication
        loader = UiLoader()
        self.ui = loader.load(os.path.join(lyse.utils.LYSE_DIR, 'user_interface/main.ui'), LyseMainWindow(self))

        self.connect_signals()

        # The singleshot routinebox will be connected to the filebox by queues:
        to_singleshot = queue.Queue()
        from_singleshot = queue.Queue()

        # So will the multishot routinebox:
        to_multishot = queue.Queue()
        from_multishot = queue.Queue()

        self.output_box = OutputBox(self.ui.verticalLayout_output_box)
        self.singleshot_routinebox = lyse.routines.RoutineBox(self, self.ui.verticalLayout_singleshot_routinebox, self.exp_config,
                                                self, to_singleshot, from_singleshot, self.output_box.port)
        self.multishot_routinebox = lyse.routines.RoutineBox(self, self.ui.verticalLayout_multishot_routinebox, self.exp_config,
                                               self, to_multishot, from_multishot, self.output_box.port, multishot=True)
        self.filebox = lyse.filebox.FileBox(self, self.ui.verticalLayout_filebox, self.exp_config,
                               to_singleshot, from_singleshot, to_multishot, from_multishot)

        self.appconfig = AppConfigActions(
            self.ui,
            'lyse configuration',
            self.get_default_save_configuration_path,
            self.ui.actionSave_configuration,
            self.ui.actionSave_configuration_as,
            self.ui.actionLoad_configuration,
            self.ui.actionRevert_configuration,
            self.get_save_data,
            self.save_configuration,
            self.load_configuration,
            lambda message: error_dialog(self.ui, 'lyse', message),
            default_load_path_getter=self.get_default_load_configuration_path,
        )
        self.ui.actionSave_dataframe_as.triggered.connect(lambda: self.on_save_dataframe_triggered(True))
        self.ui.actionSave_dataframe.triggered.connect(lambda: self.on_save_dataframe_triggered(False))
        self.ui.actionLoad_dataframe.triggered.connect(self.on_load_dataframe_triggered)
        self.ui.actionQuit.triggered.connect(self.ui.close)

        self.ui.resize(1600, 900)

        # Set the splitters to appropriate fractions of their maximum size:
        self.ui.splitter_horizontal.setSizes([1000, 600])
        self.ui.splitter_vertical.setSizes([300, 600])

        # autoload a config file, if labconfig is set to do so:
        try:
            autoload_config_file = self.exp_config.get('lyse', 'autoload_config_file')
        except (LabConfig.NoOptionError, LabConfig.NoSectionError):
            self.output_box.output('Ready.\n\n')
        else:
            self.ui.setEnabled(False)
            self.output_box.output('Loading default config file %s...' % autoload_config_file)

            def load_the_config_file():
                try:
                    self.load_configuration(autoload_config_file, restore_window_geometry)
                    self.output_box.output('done.\n')
                except Exception as e:
                    self.output_box.output('\nCould not load config file: %s: %s\n\n' %
                                           (e.__class__.__name__, str(e)), red=True)
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
        save_data = self.get_save_data()
        if self.appconfig.last_save_data is not None and save_data != self.appconfig.last_save_data:
            if self.only_window_geometry_is_different(save_data, self.appconfig.last_save_data):
                self.save_configuration(self.appconfig.last_save_config_file)
                self.terminate_all_workers()
                return True
            if not self.appconfig.prompt_to_save_if_dirty(
                'Quit lyse',
                ('Current configuration (which scripts are loaded and other GUI state) '
                 'has changed: save config file \'%s\'?'
                 % self.appconfig.last_save_config_file),
            ):
                return False
        self.terminate_all_workers()
        return True

    def get_default_save_configuration_path(self):
        """Return the default lyse save-configuration path."""

        return os.path.join(get_app_saved_configs_dir(self.exp_config, 'lyse'), 'lyse.toml')

    def get_default_load_configuration_path(self):
        """Return the default lyse load-configuration path."""

        return os.path.join(self.exp_config.get('paths', 'experiment_shot_storage'), 'lyse.toml')

    def only_window_geometry_is_different(self, current_data, old_data):
        ui_keys = ['window_size', 'window_pos', 'splitter', 'splitter_vertical', 'splitter_horizontal']
        compare = [current_data[key] == old_data[key] for key in current_data.keys() if key not in ui_keys]
        return all(compare)

    def get_save_data(self):
        save_data = {}

        box = self.singleshot_routinebox
        save_data['singleshot'] = list(zip([routine.filepath for routine in box.routines],
                                           [box.model.item(row, box.COL_ACTIVE).checkState() 
                                            for row in range(box.model.rowCount())]))
        save_data['lastsingleshotfolder'] = box.last_opened_routine_folder
        box = self.multishot_routinebox
        save_data['multishot'] = list(zip([routine.filepath for routine in box.routines],
                                          [box.model.item(row, box.COL_ACTIVE).checkState() 
                                           for row in range(box.model.rowCount())]))
        save_data['lastmultishotfolder'] = box.last_opened_routine_folder

        save_data['lastfileboxfolder'] = self.filebox.last_opened_shots_folder

        save_data['analysis_paused'] = self.filebox.analysis_paused
        window_size = self.ui.size()
        save_data['window_size'] = (window_size.width(), window_size.height())
        window_pos = self.ui.pos()

        save_data['window_pos'] = (window_pos.x(), window_pos.y())

        save_data['screen_geometry'] = lyse.utils.gui.get_screen_geometry(self.qapplication)
        save_data['splitter'] = self.ui.splitter.sizes()
        save_data['splitter_vertical'] = self.ui.splitter_vertical.sizes()
        save_data['splitter_horizontal'] = self.ui.splitter_horizontal.sizes()
        return save_data

    def save_configuration(self, save_file):
        save_data = self.get_save_data()
        save_file = save_appconfig(save_file, {'lyse_state': save_data})
        self.appconfig.mark_clean(save_file, save_data)

    def load_configuration(self, filename, restore_window_geometry=True):
        appconfig, save_target = load_appconfig(filename, return_save_path=True)
        save_data = appconfig.get('lyse_state', {})
        if 'singleshot' in save_data:
            self.singleshot_routinebox.add_routines(save_data['singleshot'], clear_existing=True)
        if 'lastsingleshotfolder' in save_data:
            self.singleshot_routinebox.last_opened_routine_folder = save_data['lastsingleshotfolder']
        if 'multishot' in save_data:
            self.multishot_routinebox.add_routines(save_data['multishot'], clear_existing=True)
        if 'lastmultishotfolder' in save_data:
            self.multishot_routinebox.last_opened_routine_folder = save_data['lastmultishotfolder']
        if 'lastfileboxfolder' in save_data:
            self.filebox.last_opened_shots_folder = save_data['lastfileboxfolder']
        if 'analysis_paused' in save_data and save_data['analysis_paused']:
            self.filebox.pause_analysis()
        if restore_window_geometry:
            self.load_window_geometry_configuration(filename)

        # Set as self.last_save_data:
        save_data = self.get_save_data()
        self.appconfig.mark_clean(save_target, save_data)

    def load_window_geometry_configuration(self, filename):
        """Load only the window geometry from the config file. It's useful to have this
        separate from the rest of load_configuration so that it can be called before the
        window is shown."""
        save_data = load_appconfig(filename).get('lyse_state', {})
        if 'screen_geometry' not in save_data:
            return
        screen_geometry = save_data['screen_geometry']
        # Only restore the window size and position, and splitter
        # positions if the screen is the same size/same number of monitors
        # etc. This prevents the window moving off the screen if say, the
        # position was saved when 2 monitors were plugged in but there is
        # only one now, and the splitters may not make sense in light of a
        # different window size, so better to fall back to defaults:
        current_screen_geometry = lyse.utils.gui.get_screen_geometry(self.qapplication)
        if list(map(tuple, current_screen_geometry)) == list(map(tuple, screen_geometry)):
            if 'window_size' in save_data:
                self.ui.resize(*save_data['window_size'])
            if 'window_pos' in save_data:
                self.ui.move(*save_data['window_pos'])
            if 'splitter' in save_data:
                self.ui.splitter.setSizes(save_data['splitter'])
            if 'splitter_vertical' in save_data:
                self.ui.splitter_vertical.setSizes(save_data['splitter_vertical'])
            if 'splitter_horizontal' in save_data:
                self.ui.splitter_horizontal.setSizes(save_data['splitter_horizontal'])

    def setup_config(self):
        required_config_params = {"default": ["apparatus_name"],
                                  "programs": ["text_editor",
                                               "text_editor_arguments",
                                               "hdf5_viewer",
                                               "hdf5_viewer_arguments"],
                                  "paths": ["shared_drive",
                                            "experiment_shot_storage",
                                            "analysislib"],
                                  "ports": ["lyse"]
                                  }
        self.exp_config = LabConfig(required_params=required_config_params)

    def connect_signals(self):
        # Keyboard shortcuts:
        QtWidgets.QShortcut('Del', self.ui, lambda: self.delete_items(True))
        QtWidgets.QShortcut('Shift+Del', self.ui, lambda: self.delete_items(False))

    def on_save_dataframe_triggered(self, choose_folder=True):
        df = self.filebox.shots_model.dataframe.copy()
        if len(df) > 0:
            default = self.exp_config.get('paths', 'experiment_shot_storage')
            if choose_folder:
                save_path = select_directory(
                    self.ui,
                    'Select a Folder for the Dataframes',
                    default,
                )
                if not save_path:
                    return
            sequences = df.sequence.unique()
            for sequence in sequences:
                sequence_df = pandas.DataFrame(df[df['sequence'] == sequence], columns=df.columns).dropna(axis=1, how='all')
                labscript = sequence_df['labscript'].iloc[0]
                filename = "dataframe_{}_{}.pkl".format(sequence.to_pydatetime().strftime("%Y%m%dT%H%M%S"),labscript[:-3])
                if not choose_folder:
                    save_path = os.path.dirname(sequence_df['filepath'].iloc[0])
                sequence_df.infer_objects()
                for col in sequence_df.columns :
                    if sequence_df[col].dtype == object:
                        sequence_df[col] = pandas.to_numeric(sequence_df[col], errors='ignore')
                sequence_df.to_pickle(os.path.join(save_path, filename))
        else:
            error_dialog(self.ui, 'lyse', 'Dataframe is empty')

    def on_load_dataframe_triggered(self):
        default = os.path.join(self.exp_config.get('paths', 'experiment_shot_storage'), 'dataframe.pkl')
        file = select_open_file(
            self.ui,
            'Select dataframe file to load',
            default,
            "dataframe files (*.pkl *.msg)",
        )
        if not file:
            return
        if file.endswith('.msg'):
            # try to read msgpack in case using older pandas
            try:
                df = pandas.read_msgpack(file).sort_values("run time").reset_index()
                # raise a deprecation warning if this succeeds
                msg = """msgpack support is being dropped by pandas >= 1.0.0.
                Please resave this dataframe to use the new format."""
                warnings.warn(dedent(msg),DeprecationWarning)
            except AttributeError as err:
                # using newer pandas that can't read msg
                msg = """msgpack is no longer supported by pandas.
                To read this dataframe, you must downgrade pandas to < 1.0.0.
                You can then read this dataframe and resave it with the new format."""
                raise DeprecationWarning(dedent(msg)) from err
        else:
            df = pandas.read_pickle(file).sort_values("run time").reset_index()
                
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

        # Reload the files where changes where made since exporting
        for index in need_updating:
            filepath = filepaths.pop(index)
            self.filebox.incoming_queue.put(filepath)
        df = df.drop(need_updating)
        
        self.filebox.shots_model.add_files(filepaths, df, done=True)

    def delete_items(self, confirm):
        """Delete items from whichever box has focus, with optional confirmation
        dialog"""
        if self.filebox.ui.tableView.hasFocus():
            self.filebox.shots_model.remove_selection(confirm)
        if self.singleshot_routinebox.ui.treeView.hasFocus():
            self.singleshot_routinebox.remove_selection(confirm)
        if self.multishot_routinebox.ui.treeView.hasFocus():
            self.multishot_routinebox.remove_selection(confirm)


if __name__ == "__main__":

    splash.update_text('starting GUI')
    qapplication = labscript_utils.splash.get_qapplication()

    app = Lyse(qapplication)

    splash.hide()
    labscript_utils.splash.run_qapplication(qapplication, on_shutdown=app.server.shutdown)
