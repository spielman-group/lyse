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

    def add_shot(self, filepath):
        """Queue a shot file to be loaded into lyse's dataframe.

        Parameters
        ----------
        filepath : str
            The shot file, as a local path or as
            :func:`labscript_utils.shared_drive.path_to_agnostic` gives it.
        """
        self.request('add_shot', filepath)

    def get_dataframe(self, n_sequences=None, filter_kwargs=None, where=None):
        """Return lyse's dataframe, or the rows of it that the arguments choose.

        Parameters
        ----------
        n_sequences, filter_kwargs, where : optional
            As for :func:`lyse.data`.

        Returns
        -------
        pandas.DataFrame

        Raises
        ------
        ValueError
            If ``where`` names a column the dataframe does not have.
        """
        return self.request(
            'get_dataframe', n_sequences=n_sequences, filter_kwargs=filter_kwargs, where=where
        )
