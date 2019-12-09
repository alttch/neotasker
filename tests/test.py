#!/usr/bin/env python3

from pathlib import Path

import sys
import logging
import unittest
import time
import threading

from types import SimpleNamespace

result = SimpleNamespace(g=None,
                         function_collection=0,
                         background_worker=0,
                         wait1=None,
                         wait2=None,
                         wait3=None,
                         background_interval_worker=0,
                         background_interval_worker_async_ex=0,
                         background_queue_worker=0,
                         background_event_worker=0,
                         test_aloop=None,
                         test_aloop_background_task=None,
                         async_js1=0,
                         async_js2=0)

sys.path.insert(0, Path(__file__).absolute().parents[1].as_posix())


def wait():
    time.sleep(0.1)


from atasker import task_supervisor, background_task, background_worker
from atasker import TT_MP, TASK_CRITICAL, wait_completed

from atasker import FunctionCollection, TaskCollection, g

from atasker import Locker, set_debug


class Test(unittest.TestCase):

    def test_g(self):

        @background_task
        def f():
            result.g = g.get('test', 222)
            g.set('ttt', 333)

        g.set('test', 1)
        g.clear('test')
        g.set('test_is', g.has('test'))
        self.assertFalse(g.get('test_is'))
        g.set('test', 999)
        f()
        wait()
        self.assertIsNone(g.get('ttt'))
        self.assertEqual(result.g, 222)

    def test_function_collection(self):

        f = FunctionCollection()

        @f
        def f1():
            result.function_collection += 1

        @f
        def f2():
            result.function_collection += 2

        f.run()
        self.assertEqual(result.function_collection, 3)

    # def test_background_worker(self):

        # @background_worker
        # def t(**kwargs):
            # result.background_worker += 1

        # t.start()
        # wait()
        # t.stop()
        # self.assertGreater(result.background_worker, 0)

    # def test_background_interval_worker(self):

        # @background_worker(interval=0.02)
        # def t(**kwargs):
            # result.background_interval_worker += 1

        # t.start()
        # wait()
        # t.stop()
        # self.assertLess(result.background_interval_worker, 10)
        # self.assertGreater(result.background_interval_worker, 4)

    # def test_background_interval_worker_async_ex(self):

        # @background_worker(interval=0.02)
        # async def t(**kwargs):
            # result.background_interval_worker_async_ex += 1

        # task_supervisor.default_aloop = None
        # t.start()
        # wait()
        # t.stop()
        # self.assertLess(result.background_interval_worker_async_ex, 10)
        # self.assertGreater(result.background_interval_worker_async_ex, 4)

    # def test_background_queue_worker(self):

        # @background_worker(q=True)
        # def t(a, **kwargs):
            # result.background_queue_worker += a

        # t.start()
        # t.put_threadsafe(2)
        # t.put_threadsafe(3)
        # t.put_threadsafe(4)
        # wait()
        # t.stop()
        # self.assertEqual(result.background_queue_worker, 9)

    # def test_background_event_worker(self):

        # @background_worker(e=True)
        # def t(**kwargs):
            # result.background_event_worker += 1

        # t.start()
        # t.trigger_threadsafe()
        # wait()
        # t.trigger_threadsafe()
        # wait()
        # t.stop()
        # self.assertEqual(result.background_event_worker, 2)

    # def test_background_interval_worker_mp(self):

        # from mpworker import TestMPWorker

        # t = TestMPWorker(interval=0.02)
        # t.start()
        # wait()
        # t.stop()
        # self.assertLess(t.a, 10)
        # self.assertGreater(t.a, 4)

    def test_supervisor(self):
        result = task_supervisor.get_info()

    def test_aloop(self):

        @background_worker(interval=0.02)
        async def t(**kwargs):
            result.test_aloop = threading.current_thread().getName()

        task_supervisor.create_aloop('test1', default=True)
        t.start()
        wait()
        t.stop()
        self.assertEqual(result.test_aloop, 'supervisor_default_aloop_test1')

    def test_async_job_scheduler(self):

        async def test1():
            result.async_js1 += 1

        async def test2():
            result.async_js2 += 1

        task_supervisor.create_aloop('jobs')
        task_supervisor.create_async_job_scheduler('default',
                                                   aloop='jobs',
                                                   default=True)
        j1 = task_supervisor.create_async_job(target=test1, interval=0.01)
        j2 = task_supervisor.create_async_job(target=test2, interval=0.01)

        time.sleep(0.1)

        task_supervisor.cancel_async_job(job=j2)

        r1 = result.async_js1
        r2 = result.async_js2

        self.assertGreater(r1, 9)
        self.assertGreater(r2, 9)

        time.sleep(0.1)

        self.assertLess(r1, result.async_js1)
        self.assertEqual(r2, result.async_js2)


task_supervisor.set_thread_pool(pool_size=20, reserve_normal=5, reserve_high=5)
task_supervisor.set_mp_pool(pool_size=20, reserve_normal=5, reserve_high=5)

if __name__ == '__main__':
    try:
        if sys.argv[1] == 'debug':
            logging.basicConfig(level=logging.DEBUG)
            set_debug()
    except:
        pass
    task_supervisor.start()
    task_supervisor.poll_delay = 0.01
    test_suite = unittest.TestLoader().loadTestsFromTestCase(Test)
    test_result = unittest.TextTestRunner().run(test_suite)
    task_supervisor.stop(wait=3)
    sys.exit(not test_result.wasSuccessful())
