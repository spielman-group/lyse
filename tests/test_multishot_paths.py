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
"""lyse.paths, the shot files a multishot routine is run for, and the attempt of a
shot: its states and the plugin calls of its analysis."""
import json
import logging
import os
import queue
import tempfile
import threading
import types
import unittest

import pandas
from qtutils.qt import QtWidgets

import lyse
import lyse.analysis_subprocess
import lyse.figure_manager
import lyse.filebox
import lyse.routines

qapplication = QtWidgets.QApplication.instance() or QtWidgets.QApplication(['test'])

ROUTINE = """\
import json, lyse, os
calls = json.load(open('calls.json')) if os.path.exists('calls.json') else []
json.dump(calls + [[os.path.basename(__file__), lyse.paths]], open('calls.json', 'w'))
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
        saved = lyse.figure_manager.figuremanager
        lyse.figure_manager.figuremanager = types.SimpleNamespace(
            reset=lambda: None, set_first_figure_current=lambda: None, figs={})
        self.addCleanup(setattr, lyse.figure_manager, 'figuremanager', saved)
        # The plugin calls, as the filebox makes them:
        self.events = []
        self.filebox = types.SimpleNamespace(
            multishot_required=True, multishot_requested=False, analysis_paused=False,
            add_shots_start_time=None, set_add_shots_progress=lambda *args: None,
            set_analysis_paused=lambda paused: None, _singleshot_outcomes={},
            fire_event=lambda *args: self.events.append(args),
        )
        app = types.SimpleNamespace(
            filebox=self.filebox, output_box=types.SimpleNamespace(output=lambda *args, **kwargs: None))
        self.filebox.shots_model = lyse.filebox.DataFrameModel(
            app, QtWidgets.QTableView(), types.SimpleNamespace(getboolean=lambda *args: False))
        self.filebox.to_singleshot, self.filebox.from_singleshot, _ = self.routine_box(False, ['singleshot.py'])
        self.filebox.to_multishot, self.filebox.from_multishot, multishot_box = self.routine_box(
            True, ['first.py', 'second.py'])
        self.filebox.app = types.SimpleNamespace(multishot_routinebox=multishot_box)

    def routine_box(self, multishot, names):
        """A routine box with a routine of each name: the filebox's pair of queues to and
        from it, and the box."""
        routines = []
        for name in names:
            path = os.path.join(self.folder, name)
            with open(path, 'w') as f:
                f.write(ROUTINE)
            worker = object.__new__(lyse.analysis_subprocess.AnalysisWorker)
            worker.filepath = path
            worker.routine_module = types.ModuleType('routine')
            worker.routine_module.__file__ = path
            worker.routine_module_clean_dict = worker.routine_module.__dict__.copy()
            worker.routine_source = None
            worker.idle, worker.routine_running = threading.Event(), False
            # In this thread, which runs_on_put needs; a real worker starts one per analysis.
            worker.analyse = worker.analysis_task
            worker.plots = {}
            worker.modulewatcher = types.SimpleNamespace(lock=threading.Lock())
            worker.to_parent = queue.Queue()
            routine = types.SimpleNamespace(shortname=name, done=False, error=False, enabled=lambda: True,
                                            from_worker=worker.to_parent, shutdown=None,
                                            worker=types.SimpleNamespace(poll=lambda: None))
            routine.to_worker = runs_on_put(worker.mainloop, worker, 'from_parent')
            routine.set_status = lambda status, routine=routine: routine.__dict__.update(
                done=status == 'done', error=status == 'error')
            routine.do_analysis = types.MethodType(lyse.routines.AnalysisRoutine.do_analysis, routine)
            routine.send_analysis = types.MethodType(lyse.routines.AnalysisRoutine.send_analysis, routine)
            routines.append(routine)
        box = types.SimpleNamespace(multishot=multishot, routines=routines,
                                    app=types.SimpleNamespace(filebox=self.filebox),
                                    to_filebox=queue.Queue(), logger=logging.getLogger('test'))
        for name in ['_start_next_routine', 'start_analysis', 'end_routine']:
            setattr(box, name, types.MethodType(getattr(lyse.routines.RoutineBox, name), box))
        box.do_analysis = types.MethodType(lyse.routines.RoutineBox.do_analysis, box)
        loop = types.MethodType(lyse.routines.RoutineBox.analysis_loop, box)
        return runs_on_put(loop, box, 'from_filebox'), box.to_filebox, box

    def analyse(self, name):
        path = os.path.join(self.folder, name)
        open(path, 'w').close()
        self.filebox.shots_model.add_files([path], pandas.DataFrame({('filepath', ''): [path]}), ['remote'])
        lyse.filebox.FileBox.do_singleshot_analysis(self.filebox, path)
        return path

    def state(self, path):
        return self.filebox.shots_model.shot_states([path])[0]

    def multishot_pass(self):
        lyse.filebox.FileBox.do_multishot_analysis(self.filebox)

    def calls(self, name):
        """What the routine of this name saw as lyse.paths, each time it ran."""
        with open(os.path.join(self.folder, 'calls.json')) as f:
            calls = json.load(f)
        return [paths for routine, paths in calls if routine == name]

    def test_a_multishot_routine_sees_the_shots_analysed_since_its_last_pass(self):
        first = self.analyse('1.h5')
        self.multishot_pass()
        second, third = self.analyse('2.h5'), self.analyse('3.h5')
        self.multishot_pass()
        self.assertEqual(self.calls('first.py'), [[first], [second, third]])

    def test_after_a_failed_routine_the_rest_run_and_its_shots_are_offered_again(self):
        first = self.analyse('1.h5')
        open(os.path.join(self.folder, 'fail'), 'w').close()  # first.py fails, once
        self.multishot_pass()
        # Its attempt has ended, and it awaits the next pass:
        self.assertEqual(self.state(first), 'awaiting multishot')
        second = self.analyse('2.h5')
        self.multishot_pass()
        for name in ['first.py', 'second.py']:
            self.assertEqual(self.calls(name), [[first], [first, second]])
        self.assertEqual([event[2] for event in self.events if event[:2] == ('analysis_done', first)],
                         [{'singleshot': 'done', 'first': 'error', 'second': 'done'},
                          {'singleshot': 'done', 'first': 'done', 'second': 'done'}])
        self.assertEqual(self.state(first), 'done')

    def test_a_shot_is_received_then_awaits_its_pass_which_ends_its_attempt(self):
        path = self.analyse('1.h5')
        self.assertEqual(self.state(path), 'awaiting multishot')
        self.multishot_pass()
        self.assertEqual(self.state(path), 'done')
        singleshot, multishot = {'singleshot': 'done'}, {'first': 'done', 'second': 'done'}
        self.assertEqual(self.events, [
            ('shot_received', path, 'remote'),
            ('singleshot_done', path, singleshot),
            ('multishot_done', [path], multishot),
            ('analysis_done', path, {**singleshot, **multishot}),
        ])

    def test_a_rerun_during_an_attempt_waits_for_it_to_end(self):
        path = self.analyse('1.h5')
        self.filebox.shots_model.mark_not_done([path])
        self.assertEqual(self.state(path), 'awaiting multishot')
        self.multishot_pass()
        self.assertEqual(self.state(path), 'not done')

    def test_a_shot_whose_file_goes_while_it_awaits_its_pass_is_not_offered(self):
        path = self.analyse('1.h5')
        os.remove(path)
        self.multishot_pass()
        self.assertEqual(self.calls('first.py'), [[]])
        self.assertEqual(self.state(path), 'deleted')
        self.assertIn(('analysis_done', path), [event[:2] for event in self.events])

    def test_a_singleshot_routine_sees_none(self):
        self.analyse('1.h5')
        self.assertEqual(self.calls('singleshot.py'), [None])


class Stop(BaseException):
    """Ends the analysis loop, which carries on through any Exception."""


class FailedPassTests(unittest.TestCase):

    def test_a_failed_pass_neither_pauses_nor_runs_again_at_once(self):
        """A pass started during singleshot analysis, by the Run multishot
        button, fails; it runs again with the next shots, not now."""
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
            analysis_paused=False, multishot_required=False, multishot_requested=False,
            quitting=False, _singleshot_outcomes={}, fire_event=lambda *args: None,
            to_multishot=queue.Queue(), from_multishot=queue.Queue(),
            shots_model=types.SimpleNamespace(
                get_first_incomplete=lambda: incomplete.pop() if incomplete else None,
                awaiting_multishot=lambda: []),
            do_singleshot_analysis=singleshot,
        )
        box.do_multishot_analysis = types.MethodType(lyse.filebox.FileBox.do_multishot_analysis, box)
        for _ in range(2):
            box.from_multishot.put(['error', 100.0, {}, {}])  # every pass fails, its routines all run
        with self.assertRaises(Stop):
            lyse.filebox.FileBox.analysis_loop(box)
        self.assertEqual(box.to_multishot.qsize(), 1)
        self.assertFalse(box.analysis_paused)

    def test_while_paused_only_the_run_button_starts_a_pass(self):
        """A pass outstanding when analysis is paused during a shot waits for resuming,
        but Run starts it now."""
        waits = []

        def wait():
            waits.append(None)
            if len(waits) == 2:
                lyse.filebox.FileBox.run_multishot(box)
            if len(waits) > 2:
                raise Stop

        def singleshot(filepath):
            box.analysis_paused = True  # the Pause button, pressed meanwhile

        incomplete = ['1.h5']
        box = types.SimpleNamespace(
            analysis_pending=types.SimpleNamespace(wait=wait, clear=lambda: None, set=lambda: None),
            analysis_paused=False, multishot_required=True, multishot_requested=False,
            quitting=False, _singleshot_outcomes={}, fire_event=lambda *args: None,
            to_multishot=queue.Queue(), from_multishot=queue.Queue(),
            shots_model=types.SimpleNamespace(
                get_first_incomplete=lambda: incomplete.pop() if incomplete else None,
                awaiting_multishot=lambda: []),
            do_singleshot_analysis=singleshot,
        )
        box.do_multishot_analysis = types.MethodType(lyse.filebox.FileBox.do_multishot_analysis, box)
        for _ in range(2):
            box.from_multishot.put(['done', 100.0, {}, {}])
        with self.assertRaises(Stop):
            lyse.filebox.FileBox.analysis_loop(box)
        self.assertEqual(box.to_multishot.qsize(), 1)


if __name__ == '__main__':
    unittest.main()
