Introduction
==============

**Lyse** is a data analysis system which gets *your code* running on experimental data as it is acquired. It is fundamentally based around the ideas of experimental *shots* and analysis *routines*. A shot is one trial of an experiment, and a routine is ``Python`` code, written by you, that does something with the measurement data from one or more shots. A routine is either a *classic script*, a ``Python`` file that **lyse** runs from top to bottom for each analysis, or a *GUI routine*, a folder of code with a window of its own for controls, figures and output (see :doc:`gui_routines`).

Analysis routines can be either *single-shot* or *multi-shot*. This determines what data and functions are available to your code when it runs. A single-shot routine has access to the data from only one shot, and functions available for saving results only to the hdf5 file for that shot. A multi-shot routine has access to the entire dataset from all the runs that are currently loaded into **lyse**, and has functions available for saving results to an hdf5 file which does not belong to any of the shots---it's a file that exists only to save the "meta results".

Actually things are far less magical than that. The only enforced difference between a single shot routine and a multi-shot routine is which of two variables **lyse** provides to your code when it runs it. A classic script runs in a perfectly clean ``Python`` environment with these exceptions: variables in the lyse namespace called ``path`` and ``paths``. If you have told **lyse** that your routine is a singleshot one, then ``path`` will be a path to the hdf5 file for the current shot being analysed, and ``paths`` will be ``None``. On the other hand, if you've told **lyse** that your routine is a multishot one, then ``path`` will be ``None``, and ``paths`` will be a list of paths to the hdf5 files of the shots analysed since the last multishot pass, which may be none. A multishot routine saves its results to an hdf5 file of its own choosing, through a :class:`~lyse.Sequence`. A GUI routine gets the same two values as ``self.path`` and ``self.paths``.

The other differences listed above are conventions only (though **lyse**'s design is based around the assumption that you'll follow these conventions most of the time), and pertain to how you use the API that **lyse** provides, which will be different depending on what sort of analysis you're doing.

The **lyse** API
~~~~~~~~~~~~~~~~~

So great, you've got a filepath, or a list of them. What data analysis could you possibly do with that? It might seem like you have to still do the same amount of work that you would without an analysis system! Whilst that's not quite true, it's intentionally been designed that way so that you can run your code outside **lyse** with very little modification. Another motivating factor is to minimise the amount of magic black box behaviour, such that an analysis routine is actually just ordinary ``Python`` code which makes use of an API designed for our purposes. **lyse** is both a program which executes your code, and an API that your code can call on.

To use the API in an analysis routine, begin your code with:

.. code-block:: python

	import lyse

The details of the API are found in the :doc:`API reference<api/_autosummary/lyse>`.

**lyse** GUI
~~~~~~~~~~~~~~~

The **lyse** GUI uses the API to apply single and multi-shot routines to collections of shot files, added either manually by the user or automatically by runmanager after shot completion.

Here's a screenshot of **lyse**:

.. _fig-gui:

.. figure:: /img/lyse_gui.png
	
	Screenshot of the Lyse GUI

* The **Singleshot routines** box, at the top left, is where single shot routines can be added and removed, with the plus and minus buttons. The plus button opens a menu with two entries: "Add classic scripts (.py)…" selects Python files, and "Add a GUI routine folder (.lyse)…" selects a folder (see :doc:`gui_routines`). Routines will be executed in order on each shot (more on how that works shortly). They can be reordered with the arrow buttons, or enabled/disabled with the checkboxes on the left. Double-clicking a routine's name opens it in your text editor, and right-clicking it opens a menu of actions that includes restarting the routine's worker process and showing its windows.

* The **Multishot routines** box, at the top right, is where multi-shot routines can be added and removed in the same way. They are given the shots analysed since their last pass as ``lyse.paths``, and choose for themselves where to save their results.

* **Analysis running** allows pausing of analysis. **lyse** by default will run all single-shot routines on a shot when it arrives (either sent by runmanager or added manually). After all the shots have been processed, only then will the multi-shot routines be executed. So if you load ten shots in quickly, the multi-shot routines won't run until they've all been processed by the single-shot routines. However most of the time there will be sufficient delay in between shots arriving that multi-shot routines will be executed pretty much every time a new shot arrives. If a routine fails, analysis pauses until you click the button again.

* **Mark as not done**: if you want to re-run single-shot analyses on some shots, select them and click this button. They'll then be processed in order.

* **Run multishot analysis** will rerun all the multi-shot analyses, whether or not new shots have arrived. ``lyse.paths`` is then the shots analysed since the last multishot pass, which may be none.

* The **Shots** table is where shots appear, either having been sent by runmanager or having been added manually via the file browser (by clicking the plus button). Many columns will populate this part of the screen, one for each global and each of the results (as saved by single-shot routines) present in the shots. The **Columns...** button chooses which columns are displayed. The data in the table, hidden columns included, is the entirety of what is available to multi-shot routines via the API provided by **lyse**.

* The output box, at the right, is where **lyse**'s own messages and the output of classic scripts are displayed, errors in red. If you're putting ``print`` statements in your analysis code, here is where to look to see them. Likewise if there's an exception and analysis stops, look here to see why. A GUI routine shows its output in a window of its own, once it has been built, so this box shows only what it prints while loading, and any loading error.

.. sectionauthor:: Chris Billington