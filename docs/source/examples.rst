Examples
==========

An analysis on a single shot
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

	from lyse import Run, data, path
	import matplotlib.pyplot as plt

	# Let's obtain our data for this shot -- globals, image attributes and
	# the results of any previously run single-shot routines:
	ser = data(path)

	# Get a global called x:
	x = ser['x']

	# Get a result saved by another single-shot analysis routine which has
	# already run. The result is called 'y', and the routine was called
	# 'some_routine':
	y = ser['some_routine','y']

	# Image attributes are also stored in this series:
	w_x2 = ser['side','absorption','OD','Gaussian_XW']

	# If we want actual measurement data, we'll have to instantiate a Run object:
	run = Run(path)

	# Obtaining a trace:
	t, mot_fluorecence = run.get_trace('mot fluorecence')

	# Now we might do some analysis on this data. Say we've written a
	# linear fit function (or we're calling some other libaries linear
	# fit function):
	m, c = linear_fit(t, mot_fluorecence)

	# We might wish to plot the fit on the trace to show whether the fit is any good:

	plt.plot(t,mot_fluorecence,label='data')
	plt.plot(t,m*t + c,label='linear fit')
	plt.xlabel('time')
	plt.ylabel('MOT flourescence')
	plt.legend()

	# There is no need to call show(): lyse will introspect what figures have
	# been made and display them once this script has finished running. lyse
	# ignores a call to show(), so the script can still call it to run outside
	# lyse. lyse keeps track of figures so that new figures replace old ones,
	# rather than you getting new window popping up every time your script runs.

	# We might wish to save this result so that we can compare it across
	# shots in a multishot analysis:
	run.save_result('mot loadrate', c)

Single shot analysis with global file opening
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

	from lyse import Run, path

	# Instantiate Run object and open
	# Globally opening the shot keeps the h5 file open
	# This prevents excessive opening and closing of the file
	# which can slow down the analysis
	with Run(path).open('r+') as shot:

		# Obtaining a trace:
		t, mot_fluorecence = shot.get_trace('mot fluorecence')

		# Now we might do some analysis on this data. Say we've written a
		# linear fit function (or we're calling some other libaries linear
		# fit function):
		m, c = linear_fit(t, mot_fluorecence)
		int_tot = mot_fluorecence.sum()
		mf_min = mot_fluorecence.min()
		mf_max = mot_fluorecence.max()

		normalised_fluorecence = normalise_response(mot_fluorecence)

		# We might wish to save this result so that we can compare it across
		# shots in a multishot analysis:
		shot.save_result('mot loadrate', c)
		shot.save_result('mot integrated', int_tot)
		shot.save_results('mot fl min', mf_min, 'mot fl max', mf_max)
		shot.save_result_array('norm mot fluorecence', normalised_fluorecence)

An analysis on multiple shots
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

	import lyse
	import matplotlib.pyplot as plt

	# Let's obtain the dataframe for all of lyse's currently loaded shots:
	df = lyse.data()

	# Now let's see how the MOT load rate varies with, say a global called
	# 'detuning', which might be the detuning of the MOT beams:

	detunings = df['detuning']

	# mot load rate was saved by a routine called calculate_load_rate:

	load_rates = df['calculate_load_rate', 'mot loadrate']

	# Let's plot them against each other:

	plt.plot(detunings, load_rates,'bo',label='data')

	# Maybe we expect a linear relationship over the range we've got:
	m, c = linear_fit(detunings, load_rates)
	# (note, not a function provided by lyse)

	plt.plot(detunings, m*detunings + c, 'ro', label='linear fit')
	plt.legend()

	#To save this result to an output hdf5 file, we have to instantiate a
	#Sequence object, giving it the path of the file to save in and the shots
	#it describes:
	seq = lyse.Sequence('path/to/results.h5', df)
	seq.save_result('detuning_loadrate_slope',c)

Moving a classic script to a GUI routine
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``lyse/examples`` folder of the lyse package holds two analyses, each written twice: as a classic script, and as a GUI routine, a folder whose name ends in ``.lyse`` (see :doc:`gui_routines`). The first computes the atom number of each shot, so add it to the Singleshot routines box. The second plots the atom numbers of the most recent sequences, so add it to the Multishot routines box. To add a GUI routine, choose "Add a GUI routine folder (.lyse)…" from the plus button's menu and select its folder. To add a classic script, choose "Add classic scripts (.py)…" and select its file.

Computing an atom number
^^^^^^^^^^^^^^^^^^^^^^^^

In ``ComputeAtomNumber.lyse`` the script's top-level code becomes ``run()``, which reads the shot with ``self.get_run()`` in place of ``lyse.Run(lyse.path)``, so its results go in a group named for the class rather than for the file. The figure is made once, in ``__init__()``, with ``add_figure()`` instead of pyplot, and is drawn by a method that ``inmain_decorator`` runs on the GUI thread, because ``run()`` does not run there. The routine also has controls, which the script lacks: the two spin boxes of ``ComputeAtomNumber.ui``, a guess of the cloud's center, are registered with ``saved_widgets()``, so lyse restores their values at the next start and saves them before each analysis.

``compute_atom_number.py``, the classic script:

.. literalinclude:: ../../lyse/examples/compute_atom_number.py
   :language: python
   :lines: 14-

``ComputeAtomNumber.lyse/lyse_routine.py``, the GUI routine:

.. literalinclude:: ../../lyse/examples/ComputeAtomNumber.lyse/lyse_routine.py
   :language: python
   :lines: 14-

Plotting the atom numbers
^^^^^^^^^^^^^^^^^^^^^^^^^

In ``PlotAtomNumber.lyse``, ``self.data()`` reads the dataframe, and the number of sequences to plot comes from a spin box, ``self.values.sequences``, instead of a constant in the script. The atom numbers are in the group named for the other routine's class, ``ComputeAtomNumber``, where the script looked in the group named for its file. As in the first pair, the figure is made once in ``__init__()``, and is drawn by a method that runs on the GUI thread.

``plot_atom_number.py``, the classic script:

.. literalinclude:: ../../lyse/examples/plot_atom_number.py
   :language: python
   :lines: 14-

``PlotAtomNumber.lyse/lyse_routine.py``, the GUI routine:

.. literalinclude:: ../../lyse/examples/PlotAtomNumber.lyse/lyse_routine.py
   :language: python
   :lines: 14-

.. sectionauthor:: Chris Billington