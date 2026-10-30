#####################################################################
#                                                                   #
# /lyse/client.py                                                   #
#                                                                   #
# Copyright 2026, JQI                                               #
# Author: Ian Spielman                                              #
#                                                                   #
# This file is part of lyse, in the labscript suite                 #
# (see http://labscriptsuite.org), and is licensed under the        #
# Simplified BSD License. See the license.txt file in the root of   #
# the project for the full license.                                 #
#                                                                   #
#####################################################################
"""The client through which other programs reach lyse's server."""
from labscript_utils.ls_zprocess import ZMQClient

from lyse.utils import LYSE_PORT


class LyseClient(ZMQClient):
    """A ZMQClient for communication with lyse."""

    server = 'lyse'
    default_port = LYSE_PORT

    def add_shot(self, filepath, source='remote'):
        """Queue a shot file to be loaded into lyse's dataframe.

        Parameters
        ----------
        filepath : str
            The shot file, as a local path or as
            :func:`labscript_utils.shared_drive.path_to_agnostic` gives it.
        source : str, optional
            Where the shot comes from, as lyse's plugins are told: ``'runmanager'``
            from runmanager, ``'remote'`` from any other client.
        """
        self.request('add_shot', filepath, source=source)

    def get_routines(self, box):
        """Return the routines of the ``'singleshot'`` or ``'multishot'`` box, in
        order, each a dict of its ``filepath``, whether it is ``active``, and the
        ``status`` of its last run: ``'working'``, ``'done'``, ``'error'`` or
        None."""
        return self.request('get_routines', box)

    def add_routines(self, box, filepaths, active=True):
        """Add routines, classic scripts (``.py``) or GUI routine folders
        (``.lyse``), to the end of a box. A routine already in the box is skipped."""
        self.request('add_routines', box, filepaths, active=active)

    def remove_routines(self, box, filepaths):
        """Remove routines from a box, ending their workers."""
        self.request('remove_routines', box, filepaths)

    def set_routines_active(self, box, filepaths, active):
        """Make routines of a box active or inactive."""
        self.request('set_routines_active', box, filepaths, active)

    def reorder_routines(self, box, filepaths):
        """Put a box's routines in the order of filepaths, which names each once."""
        self.request('reorder_routines', box, filepaths)

    def restart_routines(self, box, filepaths):
        """Restart the workers of routines of a box."""
        self.request('restart_routines', box, filepaths)

    def get_analysis_paused(self):
        """Return whether analysis is paused."""
        return self.request('get_analysis_paused')

    def set_analysis_paused(self, paused):
        """Pause or resume analysis."""
        self.request('set_analysis_paused', paused)

    def abort(self):
        """Pause analysis and interrupt the routines that are running, as the Abort
        button does."""
        self.request('abort')

    def run_multishot(self):
        """Run the multishot routines now, even if analysis is paused."""
        self.request('run_multishot')

    def rerun_shots(self, filepaths):
        """Analyse shots again, in order, as Re-run selected does."""
        self.request('rerun_shots', filepaths)

    def remove_shots(self, filepaths):
        """Remove shots from lyse's dataframe. Their files are kept."""
        self.request('remove_shots', filepaths)

    def save_dataframe(self, folder=None):
        """Save the dataframe, one file per sequence, in folder, or else beside each
        sequence's shots, as Save dataframe does, and return the files written."""
        return self.request('save_dataframe', folder)

    def load_dataframe(self, filepath):
        """Add the shots of a saved dataframe, as Load dataframe does."""
        self.request('load_dataframe', filepath)

    def get_dataframe(self, n_sequences=None, filter_kwargs=None, where=None, columns=None):
        """Return lyse's dataframe, or the rows of it that the arguments choose.

        Parameters
        ----------
        n_sequences, filter_kwargs, where, columns : optional
            As for :func:`lyse.data`.

        Returns
        -------
        pandas.DataFrame

        Raises
        ------
        ValueError
            If ``where`` names a column the dataframe does not have.
        KeyError
            If a name in ``columns`` names no column of the dataframe.
        """
        return self.request(
            'get_dataframe', n_sequences=n_sequences, filter_kwargs=filter_kwargs, where=where,
            columns=columns,
        )
