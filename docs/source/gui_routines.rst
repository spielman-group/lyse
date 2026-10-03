GUI routines
============

A *GUI routine* is an analysis routine with a window of its own. The window holds the routine's controls, figures and output. The routine is a Python object that lives for as long as its worker process does, so it can keep state from one shot to the next, and you change the analysis through its controls rather than by editing code. The analysis itself is an ordinary method, ``run()``, that reads shots, calculates, and saves results through lyse's API.

A routine added as a ``.py`` file is a *classic script*: lyse runs it from top to bottom, in a fresh namespace, for each analysis, as :doc:`the introduction <introduction>` describes. Both kinds can be loaded in either routine box, run in the order listed, and save results to the same dataframe. See :doc:`examples` for two classic scripts beside the GUI routines that replace them.

The routine folder
~~~~~~~~~~~~~~~~~~

A GUI routine is a folder whose name ends in ``.lyse``, holding its code beside the files it uses:

.. code-block:: text

    compute_OD.lyse/
        lyse_routine.py    the routine class (required)
        compute_OD.ui      Qt Designer files, any name (optional)
        fitting.py         helper modules (optional)

The folder's name is the routine's name in the routine box, in its window title and in its settings file. ``lyse_routine.py`` defines exactly one subclass of :class:`lyse.Routine <lyse.routine.Routine>`. Imported classes do not count, nor does ``lyse.Routine`` itself, and several names for one class count once. No subclass, or more than one, is an error, and a base class that several routines share belongs in an imported module.

Adding a routine
~~~~~~~~~~~~~~~~

The plus button of the Singleshot routines box, and of the Multishot routines box, opens a menu every time:

* **Add classic scripts (.py)…** selects files.
* **Add a GUI routine folder (.lyse)…** selects a folder. lyse refuses a folder whose name does not end in ``.lyse``, with a warning in its output box.

Double-clicking a routine's name opens it in the text editor that the labconfig names. For a GUI routine that opens its ``lyse_routine.py``.

A first routine
~~~~~~~~~~~~~~~

Here ``analyse_shot``, in the routine's own ``fitting.py``, is a lab's analysis function. It takes the shot path and a threshold, prints its progress, and returns a mapping that holds ``temperature``, ``x`` and ``counts``. The routine takes its threshold from a control named ``threshold`` in ``analysis.ui``, saves the temperature in its results group, ``Analysis``, and displays the result:

.. code-block:: text

    analysis.lyse/
        lyse_routine.py
        analysis.ui
        fitting.py

``lyse_routine.py``:

.. code-block:: python

    import lyse
    from qtutils import inmain_decorator
    from .fitting import analyse_shot


    class Analysis(lyse.Routine):
        def __init__(self):
            self.ui = self.load_ui("analysis.ui")
            self.saved_widgets(self.ui.threshold)
            self.axes = self.add_figure("Counts").subplots()

        def run(self):
            result = analyse_shot(self.path, self.values.threshold)
            self.get_run().save_result("temperature", result["temperature"])
            self.plot(result)

        @inmain_decorator()
        def plot(self, result):
            self.axes.clear()
            self.axes.plot(result["x"], result["counts"])
            self.axes.figure.canvas.draw_idle()

The sections below explain each part.

Analysing shots
~~~~~~~~~~~~~~~

lyse calls ``run()`` for each analysis requested from the routine's box. Analyses are sequential: lyse does not call ``run()`` again until the last call has returned and its results have been collected. An exception in ``run()`` is printed in the routine's output, and fails the analysis.

Just before each call lyse sets ``self.path`` and ``self.paths``, which mean what ``lyse.path`` and ``lyse.paths`` mean in a classic script. A single-shot routine's ``path`` is its shot file, and its ``paths`` is None. A multi-shot routine's ``paths`` are the shot files analysed since the last multi-shot pass, and its ``path`` is None. Both are None before the first analysis. ``lyse.path`` and ``lyse.paths`` hold the current shots too, but ``from lyse import path, paths`` binds the values present when the routine loads, and those never update, so use ``self.path`` and ``self.paths``.

Three methods read and save through the routine's defaults:

* ``get_run(path=None, no_write=False)`` returns a :class:`lyse.Run` for ``self.path``, or for the path it is given.
* ``get_sequence(h5_path, run_paths=None, no_write=False)`` returns a :class:`lyse.Sequence` that saves in ``h5_path`` and is associated with ``self.paths``, or with the ``run_paths`` it is given.
* ``data(filepath=None, **kwargs)`` returns :func:`lyse.data` for ``self.path``, or for the file it is given. In a multi-shot routine that is the whole dataframe. Other keyword arguments pass to :func:`lyse.data`.

Results saved through ``get_run()`` and ``get_sequence()`` go in the routine's ``group``. By default that is the class's name, and the class, or the routine at any time, may set another. A :class:`lyse.Run` made directly in a GUI routine names its group after lyse's own worker script, ``analysis_subprocess``, so a GUI routine saves through ``get_run()`` and ``get_sequence()``. ``lyse.data()``, ``lyse.Run``, ``lyse.Sequence``, ``lyse.open_file()`` and ``lyse.globals_diff()`` otherwise work as they do in a classic script.

Controls and their values
~~~~~~~~~~~~~~~~~~~~~~~~~

``load_ui(filename)`` loads a Qt Designer file, absolute or relative to the routine folder, as the window's central widget and returns it. Assign it to ``self.ui``, and its named controls are attributes of it. A routine may instead install its own central widget in ``self.window``, a ``QMainWindow``.

``saved_widgets(*widgets)`` registers the controls whose values lyse restores, saves and supplies to the analysis. It takes widget objects, not names. Each widget needs a unique ``objectName`` that is a Python name, not a keyword and not starting with an underscore, and a value that is a number, string or boolean: its Qt user property, such as a spin box's value, a line edit's text, a button's checked state or a combo box's current text. An invalid or shared name, or an unsupported value, raises ``ValueError``. Registering restores the saved value at once, so set a widget up, its range included, before registering it, and connect signals that start work only afterwards.

Just before each analysis lyse reads every registered control together, on the GUI thread, and gives ``run()`` the result as ``self.values``, with a value for each ``objectName``: ``self.values.threshold``. It holds plain values, not widgets, and the routine cannot change it. It stays fixed throughout the analysis, so a control changed during a long fit affects the next analysis, which gets a fresh one. Before the first analysis ``values`` is empty. A registered widget that has been deleted is skipped. Read a control that is not registered, or a value you want during the analysis, through the GUI-thread helpers described under Threads below.

Figures and the window
~~~~~~~~~~~~~~~~~~~~~~

The routine's window is a ``QMainWindow``. Its controls fill the central widget, and its figures and output are docks around it. Users can split, tabify or float the docks. Closing a dock hides it and keeps its figure, and the window's View menu shows it again.

``add_figure(name)`` returns a :class:`~matplotlib.figure.Figure` in the dock called ``name``, adding the dock the first time. The name is a nonempty string that identifies the dock in the saved layout. Calling again with the same name returns the same figure, so a routine normally makes its figures in ``__init__()``. It may add one later, on the GUI thread. Each dock has the navigation toolbar of lyse's plot windows, with its pan, zoom and axis controls and a clipboard action, and Ctrl+C copies the figure while focus is in the dock.

lyse neither clears nor redraws a routine's figures, so their toolbars have no Lock axes action. The routine clears or updates its axes and requests the redraw with ``figure.canvas.draw_idle()``, on the GUI thread. In a GUI routine pyplot is ordinary Matplotlib: lyse captures no pyplot figures and intercepts no pyplot calls, so a pyplot window belongs to the routine, and the routine uses it on the GUI thread too.

The ``icon`` class attribute, a path absolute or relative to the routine folder, is the icon of everything the routine shows: its windows, its Dock tile on macOS, and on Windows its own taskbar button, apart from lyse's. With ``None``, the default, the routine shows lyse's icon, and on Windows its windows group with lyse's.

Threads
~~~~~~~

Loading the routine, ``__init__()``, ``close()``, Qt slots, the reading of controls and every update of a widget or figure belong to the GUI thread. ``run()`` runs on a separate analysis thread, so the window stays responsive during a long analysis. It therefore reaches widgets and figures only through qtutils:

* :func:`~qtutils.invoke_in_main.inmain` calls a function on the GUI thread, waits, and returns its value. An exception raised there propagates to the caller.
* :func:`~qtutils.invoke_in_main.inmain_later` queues the call and returns at once.
* :func:`~qtutils.invoke_in_main.inmain_decorator` makes a method always run on the GUI thread, as ``plot()`` does in the example above.

A method called this way is ordinary routine code, and may be called whenever it is needed, including to show progress during a fit. Keep it short, to read controls or update the display, because a long calculation inside it would occupy the GUI thread and freeze the window again.

Qt slots may run while an analysis is busy, and may change controls, but they must not change state that ``run()`` owns at the same time. A routine that needs to command background work uses a queue or other synchronisation of its own. GUI callbacks and other background work do not save results: hand values destined for lyse's dataframe to ``run()`` to save. A routine may start its own threads or processes in ``__init__()``, and stops them in ``close()``. lyse owns only the analysis thread.

Output
~~~~~~

Everything the routine prints from ``run()``, from GUI methods and from its own threads appears in the Output dock of its window as it is printed, while an analysis runs, and so does output from native code and from subprocesses that inherit standard output and error. As for a classic script, lyse prints a line with the time, the routine's name and the shot before each analysis, and a blank line after the analysis.

lyse's own output box shows what is printed before the routine is built, such as top-level ``print`` calls and any loading error; what is printed once a quit has been requested, since the window closes with the worker; and lyse's own messages about the worker.

``self.output_port`` is the port of the Output dock. A routine that starts a computational child process through the suite's ``ProcessTree`` passes it as ``output_redirection_port``, and the child's output appears in the same dock. The child receives calculation inputs and returns data, not Qt widgets or figures. The routine owns the child, including its communication, stopping and cleanup.

Saved settings
~~~~~~~~~~~~~~

lyse keeps each routine's control values, window geometry and dock layout in a settings file of its own. The file is identified by the routine folder's full path, so folders with the same name in different places do not share settings. It is called ``lyse-routine-<name>-<hash>.toml``, where ``<name>`` is the folder's name without ``.lyse``, and lives in the folder of lyse's autoloaded configuration file.

lyse restores the controls when the routine registers them, and the geometry and layout once the routine is built, and then shows the window with its Output dock shown, whatever the saved layout says. You may hide the dock for the session. lyse saves the controls, geometry and layout before each analysis, with the same values that ``self.values`` holds, and when the worker quits. It does not save at every change, so an edit made after the last save may be lost if the worker is killed. A worker whose routine failed to load saves nothing, so the settings it could not use are kept.

lyse checks the file as it reads it. An invalid entry is reported and ignored, and the next save replaces it with the current value, while the other entries are restored. An unreadable file is reported and renamed aside, never over an earlier backup, and saving starts again with a new file. If it cannot be renamed it is left untouched, and nothing is saved. A failed save is reported, and neither stops an analysis nor prevents shutdown.

The settings keep your preferences across restarts. They are not a record of each analysis: save the parameters that belong with a result through ``get_run()`` or ``get_sequence()``.

Showing, restarting and quitting
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Right-clicking a routine in its box opens a menu of actions, which include:

* **show windows for selected routines** shows the routine's window, restoring it if it is minimized, and raises it. Closing the window only hides it, and leaves analysis running. Analysis never shows, restores or raises it, so a window you closed or buried stays as it is until you ask. For a classic script the action does the same for each of its plot windows.
* **restart worker process for selected routines** replaces the routine's worker. lyse loads a GUI routine's code once, when its worker starts, and does not watch files or reload modules, so restart the routine to apply an edit to ``lyse_routine.py`` or to any module in its folder. A classic script is read again for each analysis.

A routine that cannot load reports the error in lyse's output box and shows no window. That includes a folder without ``lyse_routine.py``, code that does not parse, an exception in ``__init__()``, and a file that does not define exactly one subclass. The routine fails every later analysis, reporting the error again each time, until you fix it and restart it.

When lyse quits, and when you restart or remove a routine, its worker is asked to quit. It stops starting analyses, saves the controls and layout, waits for a running ``run()`` to return, calls ``close()`` once on the GUI thread, and exits. ``close()`` releases the routine's resources and stops its own threads and processes. It must not wait for a thread that needs an ``inmain()`` call to complete, because the GUI thread cannot serve one. An error in ``close()`` is reported in lyse's output box, and the worker still quits. lyse terminates a worker that has not exited within a few seconds, so a long ``run()`` can be cut off, and ``close()`` is not guaranteed to run.

Limits
~~~~~~

lyse loads the routine folder as a package, with ``lyse_routine.py`` a module in it, so the folder's modules import one another relatively, as in ``from . import fitting``. lyse adds nothing to ``sys.path``, so a module in the folder cannot shadow a library. Code that several routines share belongs in an importable package, such as one in userlib. So does a function that you hand to ``multiprocessing``, because a spawned child process cannot import the folder's modules.
