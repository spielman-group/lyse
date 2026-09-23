"""lyse.paths: the shot files a multishot routine is run for.

The filebox hands the multishot routine box the shot files analysed since the
last multishot pass, which passes them to each routine's worker, which sets
lyse.paths.
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


class FileBoxTests(unittest.TestCase):

    def setUp(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.folder = folder.name
        self.box = types.SimpleNamespace(
            analysed_since_multishot=[],
            to_singleshot=queue.Queue(), from_singleshot=queue.Queue(),
            to_multishot=queue.Queue(), from_multishot=queue.Queue(),
            multishot_required=True,
            pause_analysis=lambda: None,
            shots_model=types.SimpleNamespace(
                update_row=lambda *args, **kwargs: None,
                set_status_percent=lambda filepath, percent: None,
            ),
        )

    def analyse(self, name, signal):
        path = os.path.join(self.folder, name)
        open(path, 'w').close()
        self.box.from_singleshot.put([signal, None, {}])
        lyse.filebox.FileBox.do_singleshot_analysis(self.box, path)
        return path

    def multishot_pass(self, signal='done'):
        self.box.from_multishot.put([signal, None, {}])
        lyse.filebox.FileBox.do_multishot_analysis(self.box)
        return self.box.to_multishot.get_nowait()

    def test_finished_files_are_handed_to_the_multishot_routines(self):
        finished = self.analyse('1.h5', 'done')
        self.analyse('2.h5', 'error')
        self.assertEqual(self.multishot_pass(), [finished])
        self.assertEqual(self.multishot_pass(), [])

    def test_a_failed_multishot_pass_puts_its_files_back(self):
        first = self.analyse('1.h5', 'done')
        self.assertEqual(self.multishot_pass('error'), [first])
        second = self.analyse('2.h5', 'done')
        self.assertEqual(self.multishot_pass(), [first, second])


class Messages:
    """The worker's queue from its parent. Raises once its messages are
    delivered, which ends the worker's loop."""

    def __init__(self, *messages):
        self.messages = list(messages)

    def get(self):
        if not self.messages:
            raise EOFError
        return self.messages.pop(0)


class RoutineBoxTests(unittest.TestCase):

    def sent(self, multishot, received):
        """What each routine's worker is sent when the box receives this."""
        routines = []
        for name in ('a.py', 'b.py'):
            routine = types.SimpleNamespace(
                shortname=name, done=False, enabled=lambda: True,
                to_worker=queue.Queue(), from_worker=queue.Queue(),
            )
            routine.set_status = lambda status, routine=routine: setattr(routine, 'done', status == 'done')
            routine.do_analysis = types.MethodType(lyse.routines.AnalysisRoutine.do_analysis, routine)
            routine.from_worker.put(['done', {}])
            routines.append(routine)
        box = types.SimpleNamespace(multishot=multishot, routines=routines, to_filebox=queue.Queue(),
                                    from_filebox=Messages(received), logger=logging.getLogger('test'))
        box.todo = types.MethodType(lyse.routines.RoutineBox.todo, box)
        box.do_analysis = types.MethodType(lyse.routines.RoutineBox.do_analysis, box)
        with self.assertRaises(EOFError):
            lyse.routines.RoutineBox.analysis_loop(box)
        return [routine.to_worker.get_nowait() for routine in routines]

    def test_a_multishot_routine_box_sends_every_routine_the_files(self):
        self.assertEqual(self.sent(True, ['1.h5']), [['analyse', (None, ['1.h5'])]] * 2)

    def test_a_singleshot_routine_box_sends_no_files(self):
        self.assertEqual(self.sent(False, 'shot.h5'), [['analyse', ('shot.h5', None)]] * 2)


class WorkerTests(unittest.TestCase):

    def setUp(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.folder = folder.name
        routine = os.path.join(self.folder, 'routine.py')
        with open(routine, 'w') as f:
            f.write("import json, lyse\n"
                    "json.dump(lyse.paths, open('seen.json', 'w'))\n")
        # Enough of a worker to run a routine; the constructor would make the
        # routine this process's __main__ module.
        self.worker = worker = object.__new__(lyse.analysis_subprocess.AnalysisWorker)
        worker.filepath = routine
        worker.routine_module = types.ModuleType('routine')
        worker.routine_module.__file__ = routine
        worker.routine_module_clean_dict = worker.routine_module.__dict__.copy()
        worker.plots = {}
        worker.modulewatcher = types.SimpleNamespace(lock=threading.Lock())
        worker.to_parent = queue.Queue()
        # Installed when the worker's process starts:
        saved = lyse.figure_manager.figuremanager
        lyse.figure_manager.figuremanager = types.SimpleNamespace(
            reset=lambda: None, set_first_figure_current=lambda: None, figs={})
        self.addCleanup(setattr, lyse.figure_manager, 'figuremanager', saved)
        lyse.analysis_subprocess.kill_lock = threading.Lock()
        self.addCleanup(delattr, lyse.analysis_subprocess, 'kill_lock')

    def seen(self, path, paths):
        self.worker.from_parent = Messages(['analyse', (path, paths)])
        with self.assertRaises(EOFError):
            self.worker.mainloop()
        self.assertEqual(self.worker.to_parent.get_nowait()[0], 'done')
        with open(os.path.join(self.folder, 'seen.json')) as f:
            return json.load(f)

    def test_a_multishot_routine_reads_its_files_as_lyse_paths(self):
        self.assertEqual(self.seen(None, ['1.h5', '2.h5']), ['1.h5', '2.h5'])

    def test_lyse_paths_is_none_in_a_singleshot_routine(self):
        lyse.utils.worker.paths = ['left from another routine.h5']
        self.assertIsNone(self.seen('shot.h5', None))


if __name__ == '__main__':
    unittest.main()
