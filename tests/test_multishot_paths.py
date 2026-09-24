#####################################################################
#                                                                   #
# /tests/test_multishot_paths.py                                    #
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
"""lyse.paths: the shot files a multishot routine is run for.

Each test runs the real chain -- filebox, routine box, routine, worker -- with
every queue replaced by one that runs the next stage when an item is put on
it, so a routine reads lyse.paths as it would inside lyse.
"""
import json
import logging
import os
import queue
import tempfile
import threading
import types
import unittest
import warnings

with warnings.catch_warnings():
    warnings.simplefilter('ignore')
    import lyse
    import lyse.analysis_subprocess
    import lyse.figure_manager
    import lyse.filebox
    import lyse.routines

ROUTINE = """\
import json, lyse, os
calls = json.load(open('calls.json')) if os.path.exists('calls.json') else []
json.dump(calls + [lyse.paths], open('calls.json', 'w'))
if os.path.exists('fail'):
    os.remove('fail')
    raise RuntimeError('this pass fails')
"""


def runs_on_put(loop, reader, attribute):
    """A queue whose put runs the loop that reads it, on that item alone."""
    def put(item):
        items = [item]

        def get():
            if not items:
                raise EOFError  # ends the loop
            return items.pop()
        setattr(reader, attribute, types.SimpleNamespace(get=get))
        try:
            loop()
        except EOFError:
            pass
    return types.SimpleNamespace(put=put)


class LysePathsTests(unittest.TestCase):

    def setUp(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.folder = folder.name
        with open(os.path.join(self.folder, 'routine.py'), 'w') as f:
            f.write(ROUTINE)
        saved = lyse.figure_manager.figuremanager
        lyse.figure_manager.figuremanager = types.SimpleNamespace(
            reset=lambda: None, set_first_figure_current=lambda: None, figs={})
        self.addCleanup(setattr, lyse.figure_manager, 'figuremanager', saved)
        lyse.analysis_subprocess.kill_lock = threading.Lock()
        self.addCleanup(delattr, lyse.analysis_subprocess, 'kill_lock')
        self.filebox = types.SimpleNamespace(
            analysed_since_multishot=[], multishot_required=True, pause_analysis=lambda: None,
            shots_model=types.SimpleNamespace(update_row=lambda *a, **k: None,
                                              set_status_percent=lambda *a: None),
        )
        self.filebox.to_singleshot, self.filebox.from_singleshot = self.routine_box(multishot=False)
        self.filebox.to_multishot, self.filebox.from_multishot = self.routine_box(multishot=True)

    def routine_box(self, multishot):
        """A routine box with the one routine, as the filebox's pair of queues."""
        path = os.path.join(self.folder, 'routine.py')
        worker = object.__new__(lyse.analysis_subprocess.AnalysisWorker)
        worker.filepath = path
        worker.routine_module = types.ModuleType('routine')
        worker.routine_module.__file__ = path
        worker.routine_module_clean_dict = worker.routine_module.__dict__.copy()
        worker.plots = {}
        worker.modulewatcher = types.SimpleNamespace(lock=threading.Lock())
        worker.to_parent = queue.Queue()
        routine = types.SimpleNamespace(shortname='routine.py', done=False, enabled=lambda: True,
                                        from_worker=worker.to_parent)
        routine.to_worker = runs_on_put(worker.mainloop, worker, 'from_parent')
        routine.set_status = lambda status: setattr(routine, 'done', status == 'done')
        routine.do_analysis = types.MethodType(lyse.routines.AnalysisRoutine.do_analysis, routine)
        box = types.SimpleNamespace(multishot=multishot, routines=[routine],
                                    to_filebox=queue.Queue(), logger=logging.getLogger('test'))
        box.todo = types.MethodType(lyse.routines.RoutineBox.todo, box)
        box.do_analysis = types.MethodType(lyse.routines.RoutineBox.do_analysis, box)
        loop = types.MethodType(lyse.routines.RoutineBox.analysis_loop, box)
        return runs_on_put(loop, box, 'from_filebox'), box.to_filebox

    def analyse(self, name):
        path = os.path.join(self.folder, name)
        open(path, 'w').close()
        lyse.filebox.FileBox.do_singleshot_analysis(self.filebox, path)
        return path

    def multishot_pass(self):
        lyse.filebox.FileBox.do_multishot_analysis(self.filebox)

    def calls(self, multishot=True):
        """What the routine saw as lyse.paths, run as a multishot routine or as
        a singleshot one, which sees None."""
        with open(os.path.join(self.folder, 'calls.json')) as f:
            calls = json.load(f)
        return [c for c in calls if (c is not None) == multishot]

    def test_a_multishot_routine_sees_the_shots_analysed_since_its_last_pass(self):
        first = self.analyse('1.h5')
        self.multishot_pass()
        second, third = self.analyse('2.h5'), self.analyse('3.h5')
        self.multishot_pass()
        self.assertEqual(self.calls(), [[first], [second, third]])

    def test_after_a_failed_pass_its_shots_are_offered_again(self):
        first = self.analyse('1.h5')
        open(os.path.join(self.folder, 'fail'), 'w').close()
        self.multishot_pass()
        second = self.analyse('2.h5')
        self.multishot_pass()
        self.assertEqual(self.calls(), [[first], [first, second]])

    def test_a_singleshot_routine_sees_none(self):
        self.analyse('1.h5')
        self.assertEqual(self.calls(multishot=False), [None])


class Stop(BaseException):
    """Ends the analysis loop, which carries on through any Exception."""


class FailedPassTests(unittest.TestCase):

    def test_a_failed_pass_is_not_run_again_until_analysis_resumes(self):
        """A pass started during singleshot analysis, by the Run multishot
        button, fails and pauses analysis; it runs again on resuming."""
        waits = []

        def wait():
            waits.append(None)
            if len(waits) > 1:
                raise Stop

        def singleshot(filepath):
            box.multishot_required = True  # the button, pressed meanwhile

        incomplete = ['1.h5']
        box = types.SimpleNamespace(
            analysis_pending=types.SimpleNamespace(wait=wait, clear=lambda: None),
            analysis_paused=False, multishot_required=False, analysed_since_multishot=[],
            to_multishot=queue.Queue(), from_multishot=queue.Queue(),
            pause_analysis=lambda: setattr(box, 'analysis_paused', True),
            shots_model=types.SimpleNamespace(
                get_first_incomplete=lambda: incomplete.pop() if incomplete else None),
            do_singleshot_analysis=singleshot,
        )
        box.do_multishot_analysis = types.MethodType(lyse.filebox.FileBox.do_multishot_analysis, box)
        for _ in range(2):
            box.from_multishot.put(['error', None, {}])  # every pass fails
        with self.assertRaises(Stop):
            lyse.filebox.FileBox.analysis_loop(box)
        self.assertEqual(box.to_multishot.qsize(), 1)


if __name__ == '__main__':
    unittest.main()
