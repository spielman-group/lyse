#####################################################################
#                                                                   #
# /analysis_subprocess.py                                           #
#                                                                   #
# Copyright 2013, Monash University                                 #
#                                                                   #
# This file is part of the program lyse, in the labscript suite     #
# (see http://labscriptsuite.org), and is licensed under the        #
# Simplified BSD License. See the license.txt file in the root of   #
# the project for the full license.                                 #
#                                                                   #
#####################################################################
"""Analysis subprocess definitions and routines
"""
 
import labscript_utils.excepthook # I do magic stuff, so import must be in place
import labscript_utils.h5_lock, h5py
import labscript_utils.splash

from labscript_utils.ls_zprocess import ProcessTree

import sys
import os
import ctypes.util
import importlib
import queue
import threading
import traceback
import time
from pathlib import Path
from types import ModuleType

from qtutils.qt import QtCore, QtGui, QtWidgets
from qtutils.qt.QtCore import pyqtSignal as Signal
# LEGACY INI COMPATIBILITY. DEPRECATED CODE, WILL BE REMOVED.
from qtutils.qt.QtCore import QByteArray, QSettings

from qtutils import inmain, inmain_decorator
import qtutils.icons

import multiprocessing

from matplotlib.backends.backend_qt5agg import NavigationToolbar2QT as NavigationToolbar

# Labscript imports
from labscript_utils.modulewatcher import ModuleWatcher
from labscript_utils.qtwidgets.outputbox import OutputBox
from labscript_utils import dedent
from labscript_utils.labconfig import (
    backup_legacy_config,
    load_appconfig,
    save_appconfig,
)


# Associate app windows with OS menu shortcuts:
import desktop_app
desktop_app.set_process_appid('lyse')

# lyse imports
import lyse.utils
import lyse.utils.gui
import lyse.utils.worker
import lyse.figure_manager
import lyse.routine

# This process is not fork-safe. Spawn fresh processes on platforms that would fork:
if (
    hasattr(multiprocessing, 'get_start_method')
    and multiprocessing.get_start_method(True) != 'spawn'
):
    multiprocessing.set_start_method('spawn')

# read labconfig once for plot window locations
autoload_config_file = lyse.utils.LABCONFIG.get('lyse', 'autoload_config_file')
config_dir = os.path.dirname(autoload_config_file)

class PlotWindowCloseEvent(QtGui.QCloseEvent):
    def __init__(self, force, *args, **kwargs):
        QtGui.QCloseEvent.__init__(self, *args, **kwargs)
        self.force = force

class PlotWindow(lyse.utils.gui.ThemedWindow, QtWidgets.QWidget):
    # A signal for when the window manager has created a new window for this widget:
    close_signal = Signal()

    def __init__(self, plot, *args, **kwargs):
        self.__plot = plot
        filepath = kwargs.pop('analysis_filepath')
        self.identifier = kwargs.pop('analysis_identifier')
        QtWidgets.QWidget.__init__(self, *args, **kwargs)

        # configure plot window persistence
        filename = os.path.basename(os.path.splitext(filepath)[0])
        self.settings_path = os.path.join(config_dir, 'lyse-' + filename)

        self.restore_geometry()

    def _geometry_key(self):
        return f"windowGeometry-{self.identifier}"

    # LEGACY INI COMPATIBILITY. DEPRECATED CODE, WILL BE REMOVED.
    def _legacy_settings_path(self):
        return self.settings_path + '.ini'

    def _toml_settings_path(self):
        return self.settings_path + '.toml'

    def _decode_geometry(self, geometry):
        if geometry is None:
            return None
        if isinstance(geometry, QByteArray):
            return geometry if not geometry.isEmpty() else None
        if isinstance(geometry, str):
            geometry = QByteArray.fromBase64(geometry.encode('ascii'))
            return geometry if not geometry.isEmpty() else None
        return None

    def _load_state(self):
        toml_path = self._toml_settings_path()
        if not os.path.exists(toml_path):
            return {}
        return load_appconfig(toml_path).get('lyse_plot_window_state', {})

    # LEGACY INI COMPATIBILITY. DEPRECATED CODE, WILL BE REMOVED.
    def _migrate_legacy_geometry(self, state):
        """Migrate legacy QSettings geometry into TOML app config."""
        legacy_path = self._legacy_settings_path()
        if not os.path.exists(legacy_path):
            return state
        settings = QSettings(legacy_path, QSettings.IniFormat)
        migrated = dict(state)
        for key in settings.allKeys():
            geometry = settings.value(key, QByteArray())
            geometry = self._decode_geometry(geometry)
            if geometry is None:
                continue
            migrated.setdefault(
                str(key), bytes(geometry.toBase64()).decode('ascii')
            )
        if migrated != state:
            save_appconfig(self.settings_path, {'lyse_plot_window_state': migrated})
        settings.sync()
        backup_legacy_config(legacy_path)
        return migrated

    def closeEvent(self, event):
        self.hide()
        if isinstance(event, PlotWindowCloseEvent) and event.force:
            self.save_geometry()
            self.__plot.on_close()
            event.accept()
        else:
            event.ignore()

    def restore_geometry(self):
        """Restores window geometry from local plot-window config.
        
        Will do nothing if config not present.
        """
        state = self._load_state()
        geometry = self._decode_geometry(state.get(self._geometry_key()))
        if geometry is None:
            # LEGACY INI COMPATIBILITY. DEPRECATED CODE, WILL BE REMOVED.
            state = self._migrate_legacy_geometry(state)
            geometry = self._decode_geometry(state.get(self._geometry_key()))
        if geometry is not None:
            self.restoreGeometry(geometry)

    def save_geometry(self):
        # LEGACY INI COMPATIBILITY. DEPRECATED CODE, WILL BE REMOVED.
        state = self._migrate_legacy_geometry(self._load_state())
        geometry = bytes(self.saveGeometry().toBase64()).decode('ascii')
        state[self._geometry_key()] = geometry
        save_appconfig(self.settings_path, {'lyse_plot_window_state': state})


class Plot(object):
    def __init__(self, figure, identifier, filepath):
        self.identifier = identifier
        self.ui = lyse.routine.loader.load(
            os.path.join(lyse.utils.LYSE_DIR, 'user_interface/plot_window.ui'),
            PlotWindow(self, analysis_filepath=filepath, analysis_identifier=identifier))

        self.set_window_title(identifier, filepath)

        # figure.tight_layout()
        self.figure = figure
        self.canvas = figure.canvas
        self.navigation_toolbar = NavigationToolbar(self.canvas, self.ui)

        self.lock_action = self.navigation_toolbar.addAction(
            QtGui.QIcon(':qtutils/fugue/lock-unlock'),
           'Lock axes', self.on_lock_axes_triggered)
        self.lock_action.setCheckable(True)
        self.lock_action.setToolTip('Lock axes')

        self.copy_to_clipboard_action = lyse.routine.fill_plot_form(
            self.ui, self.navigation_toolbar, self.on_copy_to_clipboard_triggered)
        self.copy_to_clipboard_action.setShortcut(QtGui.QKeySequence.Copy)

        self.lock_axes = False
        self.axis_limits = None

        self.update_window_size()

        self.ui.show()

    def on_lock_axes_triggered(self):
        if self.lock_action.isChecked():
            self.lock_axes = True
            self.lock_action.setIcon(QtGui.QIcon(':qtutils/fugue/lock'))
        else:
            self.lock_axes = False
            self.lock_action.setIcon(QtGui.QIcon(':qtutils/fugue/lock-unlock'))

    def on_copy_to_clipboard_triggered(self):
        lyse.utils.worker.figure_to_clipboard(self.figure)

    @inmain_decorator()
    def save_axis_limits(self):
        axis_limits = {}
        for i, ax in enumerate(self.figure.axes):
            # Save the limits of the axes to restore them afterward:
            axis_limits[i] = ax.get_xlim(), ax.get_ylim()

        self.axis_limits = axis_limits

    @inmain_decorator()
    def clear(self):
        self.figure.clear()

    @inmain_decorator()
    def restore_axis_limits(self):
        for i, ax in enumerate(self.figure.axes):
            try:
                xlim, ylim = self.axis_limits[i]
                ax.set_xlim(xlim)
                ax.set_ylim(ylim)
            except KeyError:
                continue

    @inmain_decorator()
    def set_window_title(self, identifier, filepath):
        self.ui.setWindowTitle(str(identifier) + ' - ' + os.path.basename(filepath))

    @inmain_decorator()
    def update_window_size(self):
        l, w = self.figure.get_size_inches()
        dpi = self.figure.get_dpi()
        self.canvas.resize(int(l*dpi),int(w*dpi))
        self.ui.adjustSize()

    @inmain_decorator()
    def draw(self):
        self.canvas.draw()

    def show(self):
        self.ui.show()

    @property
    def is_shown(self):
        return self.ui.isVisible()

    def analysis_complete(self, figure_in_use):
        """To be overriden by subclasses. 
        Called as part of the post analysis plot actions"""
        pass

    def get_window_state(self):
        """Called when the Plot window is about to be closed due to a change in 
        registered Plot window class

        Can be overridden by subclasses if custom information should be saved
        (although bear in mind that you will passing the information from the previous 
        Plot subclass which might not be what you want unless the old and new classes
        have a common ancestor, or the change in Plot class is triggered by a reload
        of the module containing your Plot subclass). 

        Returns a dictionary of information on the window state.

        If you have overridden this method, please call the base method first and
        then update the returned dictionary with your additional information before 
        returning it from your method.
        """
        return {
            'window_geometry': self.ui.saveGeometry(),
            'axis_lock_state': self.lock_axes,
            'axis_limits': self.axis_limits,
        }

    def restore_window_state(self, state):
        """Called when the Plot window is recreated due to a change in registered
        Plot window class.

        Can be overridden by subclasses if custom information should be restored
        (although bear in mind that you will get the information from the previous 
        Plot subclass which might not be what you want unless the old and new classes
        have a common ancestor, or the change in Plot class is triggered by a reload
        of the module containing your Plot subclass). 

        If overriding, please call the parent method in addition to your new code

        Arguments:
            state: A dictionary of information to restore
        """
        geometry = state.get('window_geometry', None)
        if geometry is not None:
            self.ui.restoreGeometry(geometry)

        axis_limits = state.get('axis_limits', None)
        axis_lock_state = state.get('axis_lock_state', None)
        if axis_lock_state is not None:
            if axis_lock_state:
                # assumes the default state is False for new windows
                self.lock_action.trigger()

                if axis_limits is not None:
                    self.axis_limits = axis_limits
                    self.restore_axis_limits()

    def on_close(self):
        """Called when the window is closed.

        Note that this only happens if the Plot window class has changed. 
        Clicking the "X" button in the window title bar has been overridden to hide
        the window instead of closing it."""
        # release selected toolbar action as selecting an action acquires a lock
        # that is associated with the figure canvas (which is reused in the new
        # plot window) and this must be released before closing the window or else
        # it is held forever
        self.navigation_toolbar.pan()
        self.navigation_toolbar.zoom()
        self.navigation_toolbar.pan()
        self.navigation_toolbar.pan()


class AnalysisWorker(object):
    def __init__(self, filepath, to_parent, from_parent):
        self.to_parent = to_parent
        self.from_parent = from_parent
        self.filepath = filepath

        # Add user script directory to the pythonpath:
        sys.path.insert(0, os.path.dirname(self.filepath))
        
        # Create a module for the user's routine, and insert it into sys.modules as the
        # __main__ module:
        self.routine_module = ModuleType('__main__')
        self.routine_module.__file__ = self.filepath
        # Save the dict so we can reset the module to a clean state later:
        self.routine_module_clean_dict = self.routine_module.__dict__.copy()
        sys.modules[self.routine_module.__name__] = self.routine_module

        # Plot objects, keyed by matplotlib Figure object:
        self.plots = {}

        # An object with a method to unload user modules if any have
        # changed on disk:
        self.modulewatcher = ModuleWatcher()
        
        # Start the thread that listens for instructions from the
        # parent process:
        self.mainloop_thread = threading.Thread(target=self.mainloop)
        self.mainloop_thread.daemon = True
        self.mainloop_thread.start()
        
    def mainloop(self):
        # HDF5 prints lots of errors by default, for things that aren't
        # actually errors. These are silenced on a per thread basis,
        # and automatically silenced in the main thread when h5py is
        # imported. So we'll silence them in this thread too:
        h5py._errors.silence_errors()
        while True:
            task, data = self.from_parent.get()
            if task == 'quit':
                self.quit()
            elif task == 'analyse':
                self.analyse(*data)
            elif task == 'show':
                self.show_windows()
            else:
                self.to_parent.put(['error','invalid task %s'%str(task)])

    def quit(self):
        with kill_lock:
            self.close_plots()
            inmain(qapplication.quit)

    def analyse(self, path, paths):
        with kill_lock:
            self.reply(self.do_analysis(path, paths))

    def reply(self, success):
        if success:
            if lyse.utils.worker._delay_flag:
                lyse.utils.worker.delay_event.wait()
            self.to_parent.put(['done', lyse.utils.worker._updated_data])
        else:
            self.to_parent.put(['error', lyse.utils.worker._updated_data])

    def reset_results(self, path, paths):
        # global variables used to communicate between analysis processes and GUI functions
        lyse.utils.worker.path = path
        lyse.utils.worker.paths = paths
        lyse.utils.worker._updated_data = {}
        lyse.utils.worker._delay_flag = False
        lyse.utils.worker.delay_event.clear()

    def print_header(self, path):
        now = time.strftime('[%x %X]')
        if path is not None:
            print('%s %s %s ' %(now, os.path.basename(self.filepath), os.path.basename(path)))
        else:
            print('%s %s' %(now, os.path.basename(self.filepath)))

    def windows(self):
        return [plot.ui for plot in self.plots.values()]

    @inmain_decorator()
    def show_windows(self):
        for window in self.windows():
            # Clears only the minimized state, so that a maximized window stays maximized.
            window.setWindowState(window.windowState() & ~QtCore.Qt.WindowState.WindowMinimized)
            window.show()
            window.raise_()
            window.activateWindow()

    @inmain_decorator()
    def close_plots(self):
        """Ensures analysis plots get the force close event and save geometry when lyse closes"""
        for plot in self.plots.values():
            event = PlotWindowCloseEvent(True)
            QtCore.QCoreApplication.instance().postEvent(plot.ui, event)
        
    @inmain_decorator()
    def do_analysis(self, path, paths):
        self.print_header(path)

        self.pre_analysis_plot_actions()

        # Reset the routine module's namespace:
        self.routine_module.__dict__.clear()
        self.routine_module.__dict__.update(self.routine_module_clean_dict)

        self.reset_results(path, paths)
        lyse.utils.worker.plots = self.plots
        lyse.utils.worker.Plot = Plot

        # Save the current working directory before changing it to the
        # location of the user's script:
        cwd = os.getcwd()
        os.chdir(os.path.dirname(self.filepath))

        # Do not let the modulewatcher unload any modules whilst we're working:
        try:
            with self.modulewatcher.lock:
                # Actually run the user's analysis!
                with open(self.filepath) as f:
                    code = compile(
                        f.read(),
                        self.routine_module.__file__,
                        'exec',
                        dont_inherit=True,
                    )
                    exec(code, self.routine_module.__dict__)
        except Exception:
            traceback_lines = traceback.format_exception(*sys.exc_info())
            print('\n'.join(traceback_lines[1:]), file=sys.stderr)
            return False
        else:
            return True
        finally:
            os.chdir(cwd)
            print('')
            self.post_analysis_plot_actions()
        
    def pre_analysis_plot_actions(self):
        lyse.figure_manager.figuremanager.reset()
        for plot in self.plots.values():
            plot.save_axis_limits()
            plot.clear()

    def post_analysis_plot_actions(self):
        # reset the current figure to figure 1:
        lyse.figure_manager.figuremanager.set_first_figure_current()
        # Introspect the figures that were produced:
        for identifier, fig in lyse.figure_manager.figuremanager.figs.items():
            window_state = None
            if not fig.axes:
                # Try and clear the figure if it is not in use
                try:
                    plot = self.plots[fig]
                    plot.set_window_title("Empty", self.filepath)
                    plot.draw()
                    plot.analysis_complete(figure_in_use=False)
                except KeyError:
                    pass
                # Skip the rest of the loop regardless of whether we managed to clear
                # the unused figure or not!
                continue
            try:
                plot = self.plots[fig]

                # Get the Plot subclass registered for this plot identifier if it exists
                cls = lyse.utils.worker.get_plot_class(identifier)
                # If no plot was registered, use the base class
                if cls is None: cls = Plot
                
                # if plot instance does not match the expected identifier,  
                # or the identifier in use with this plot has changes,
                #  we need to close and reopen it!
                if type(plot) != cls or plot.identifier != identifier:
                    window_state = plot.get_window_state()

                    # Create a custom CloseEvent to force close the plot window
                    event = PlotWindowCloseEvent(True)
                    QtCore.QCoreApplication.instance().postEvent(plot.ui, event)
                    # Delete the plot
                    del self.plots[fig]

                    # force raise the keyerror exception to recreate the window
                    self.plots[fig]

            except KeyError:
                # If we don't already have this figure, make a window
                # to put it in:
                plot = self.new_figure(fig, identifier)

                # restore window state/geometry if it was saved
                if window_state is not None:
                    plot.restore_window_state(window_state)
            else:
                if not plot.is_shown:
                    plot.show()
                    plot.update_window_size()
                plot.set_window_title(identifier, self.filepath)
                if plot.lock_axes:
                    plot.restore_axis_limits()
                plot.draw()
            plot.analysis_complete(figure_in_use=True)


    def new_figure(self, fig, identifier):
        try:
            # Get custom class for this plot if it is registered
            cls = lyse.utils.worker.get_plot_class(identifier)
            # If no plot was registered, use the base class
            if cls is None: cls = Plot
            # if cls is not a subclass of Plot, then raise an Exception
            if not issubclass(cls, Plot): 
                raise RuntimeError('The specified class must be a subclass of lyse.utils.worker.Plot')
            # Instantiate the plot
            self.plots[fig] = cls(fig, identifier, self.filepath)
        except Exception:
            traceback_lines = traceback.format_exception(*sys.exc_info())
            message = """Failed to instantiate custom class for plot "{identifier}".
                Perhaps lyse.register_plot_class() was called incorrectly from your
                script? The exception raised was:
                """.format(identifier=identifier)
            message = dedent(message) + '\n'.join(traceback_lines[1:])
            message += '\n'
            message += 'Due to this error, we used the default lyse.Plot class instead.\n'
            sys.stderr.write(message)

            # instantiate plot using original Base class so that we always get a plot
            self.plots[fig] = Plot(fig, identifier, self.filepath)

        return self.plots[fig]

    def reset_figs(self):
        pass


class GuiWorker(AnalysisWorker):
    """The worker of a GUI routine, a folder whose name ends in .lyse. The routine loads once as
    the worker starts; if that fails, every analysis reports the error and fails until restart."""

    routine = None

    def __init__(self, folder, to_parent, from_parent, active):
        # Not the base class's: it starts a ModuleWatcher, which reloads code, and the command
        # listener, which must wait until the routine has loaded.
        self.to_parent, self.from_parent, self.filepath = to_parent, from_parent, folder
        self.settings = lyse.routine.RoutineSettings(folder, config_dir)
        controls, layout = self.settings.load()
        # Output goes to lyse's box until the routine is built, so that a failure shows there.
        try:
            if not Path(folder, 'lyse_routine.py').is_file():
                raise FileNotFoundError(f'The routine folder {folder} has no lyse_routine.py.')
            os.chdir(folder)
            # The folder is a package, so that its modules import one another relatively and
            # nothing is added to sys.path.
            package = sys.modules['lyse_routine'] = ModuleType('lyse_routine')
            package.__path__ = [folder]
            module = importlib.import_module('lyse_routine.lyse_routine')
            cls = lyse.routine.routine_class(vars(module))
            # Windows stamps a window with the appid current as it creates it, so the routine's
            # icon and appid come before its window.
            icon = Path(folder, cls.icon) if cls.icon else lyse.utils.LYSE_DIR / 'lyse.svg'
            qapplication.setWindowIcon(QtGui.QIcon(str(icon)))
            if cls.icon and desktop_app.environment.WINDOWS:
                desktop_app.windows.set_process_appusermodel_id(
                    f'lyse.routine.{self.settings.digest}')
            self.window = lyse.routine.RoutineWindow()
            self.window.setWindowTitle(os.path.basename(folder))
            box = OutputBox(self.window.verticalLayout_output)
            self.routine = lyse.routine.construct(cls, controls, self.window, folder, box.port)
        except Exception:
            self.error = traceback.format_exc()
            print(self.error, file=sys.stderr)
        else:
            lyse.routine.route_output(box.port)
            lyse.routine.restore_layout(self.window, layout)
            # An inactive routine's window stays hidden until the user shows it.
            if active:
                self.window.show()
            self.analyses = queue.Queue()
            self.analysis_thread = threading.Thread(target=self.analysis_loop, daemon=True)
            self.analysis_thread.start()
        threading.Thread(target=self.mainloop, daemon=True).start()

    def windows(self):
        # A routine that failed to load has no window.
        return [] if self.routine is None else [self.window]

    def analyse(self, path, paths):
        if self.routine is None:
            self.print_header(path)
            print(self.error, file=sys.stderr)
            print('')
            self.to_parent.put(['error', {}])
        else:
            self.analyses.put((path, paths))

    def analysis_loop(self):
        # Silenced per thread, as in mainloop().
        h5py._errors.silence_errors()
        for path, paths in iter(self.analyses.get, None):
            with kill_lock:
                self.reset_results(path, paths)
                try:
                    self.print_header(path)
                    self.routine.values = self.save_controls()
                    self.routine.path, self.routine.paths = path, paths
                    self.routine.run()
                    success = True
                except Exception:
                    traceback.print_exc()
                    success = False
                print('')
                self.reply(success)

    @inmain_decorator()
    def save_controls(self):
        """Save the controls and the layout, and return the controls' values."""
        values, controls = lyse.routine.read_saved_widgets(self.routine)
        self.settings.save(controls, lyse.routine.save_layout(self.window))
        return values

    def quit(self):
        if self.routine is not None:
            self.analyses.put(None)
            try:
                # The dock goes with the worker, so output from here on goes to lyse's box.
                if port := process_tree.output_redirection_port:
                    lyse.routine.route_output(port)
                self.save_controls()
            except Exception:
                traceback.print_exc()
            self.analysis_thread.join()
        # Held only after the join, so that lyse can still terminate a run that hangs.
        with kill_lock:
            try:
                if self.routine is not None:
                    inmain(self.routine.close)
            except Exception:
                traceback.print_exc()
            inmain(qapplication.quit)


class DockIcon(QtCore.QObject):
    """Gives a macOS process a Dock icon only while one of its windows is visible."""
    policy = None

    def eventFilter(self, obj, event):
        # Decide once the events are over, so a window hidden and shown again keeps its icon.
        if (event.type() in (QtCore.QEvent.Type.Show, QtCore.QEvent.Type.Hide)
                and obj.isWidgetType() and obj.isWindow()):
            QtCore.QTimer.singleShot(0, self.set_policy)
        return False

    def set_policy(self):
        # Policy 0, Regular, has a Dock icon. Policy 1, Accessory, has none but, unlike
        # Prohibited, lets a window shown later take focus.
        visible = any(window.isVisible() for window in QtWidgets.QApplication.topLevelWidgets())
        policy = 0 if visible else 1
        if policy == self.policy:
            return
        self.policy = policy
        appkit = ctypes.CDLL(ctypes.util.find_library('AppKit'))
        appkit.sel_registerName.restype = ctypes.c_void_p
        appkit.objc_msgSend.argtypes = [ctypes.c_void_p, ctypes.c_void_p, ctypes.c_long]
        appkit.objc_msgSend(ctypes.c_void_p.in_dll(appkit, 'NSApp'),
                            appkit.sel_registerName(b'setActivationPolicy:'), policy)
        if policy == 0:
            # The Dock drops an icon sent while the process had no tile, so send it to the new one.
            QtWidgets.QApplication.setWindowIcon(QtWidgets.QApplication.windowIcon())


if __name__ == '__main__':

    lyse.utils.worker.spinning_top = True

    process_tree = ProcessTree.connect_to_parent()
    to_parent = process_tree.to_parent
    from_parent = process_tree.from_parent
    kill_lock = process_tree.kill_lock
    filepath, active = from_parent.get()
    gui = Path(filepath).suffix == lyse.utils.GUI_ROUTINE_SUFFIX
    if not gui:
        # Only a classic worker captures pyplot's figures.
        os.environ['MPLBACKEND'] = "qt5agg"
        lyse.figure_manager.install()
        # Where are pylab features used?  Should this be here?
        import pylab

    # Rename this module to _analysis_subprocess and put it in sys.modules
    # under that name. Only a classic script's routine will become the __main__ module;
    # a GUI routine is imported as a package.
    __name__ = '_analysis_subprocess'

    sys.modules[__name__] = sys.modules['__main__']

    # Set a meaningful client id for zlock
    process_tree.zlock_client.set_process_name('lyse-'+os.path.basename(filepath))

    if sys.platform == 'darwin':
        # Qt would make the process a Dock application as soon as its QApplication exists.
        os.environ['QT_MAC_DISABLE_FOREGROUND_APPLICATION_TRANSFORM'] = '1'
    qapplication = QtWidgets.QApplication.instance()
    if qapplication is None:
        qapplication = QtWidgets.QApplication(sys.argv)
    if sys.platform == 'darwin':
        dock_icon = DockIcon(qapplication)
        qapplication.installEventFilter(dock_icon)
        dock_icon.set_policy()
    qapplication.setProperty(
        '_labscript_icon_path', os.path.join(lyse.utils.LYSE_DIR, 'lyse.svg')
    )
    qapplication.setApplicationName('lyse')
    qapplication.setApplicationDisplayName('lyse')
    labscript_utils.splash.configure_qapplication(qapplication)
    if gui:
        worker = GuiWorker(filepath, to_parent, from_parent, active)
    else:
        worker = AnalysisWorker(filepath, to_parent, from_parent)
    qapplication.exec()
        
