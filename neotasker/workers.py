import threading
import logging
import uuid
import time
import asyncio
import queue
import types

from neotasker import task_supervisor
from neotasker.supervisor import ALoop

import concurrent.futures

logger = logging.getLogger('neotasker')

debug = False


class BackgroundWorker:

    # ----- override this -----

    def run(self, *args, **kwargs):
        raise Exception('not implemented')

    def before_start(self):
        pass

    def send_stop_events(self):
        pass

    def after_start(self):
        pass

    def before_stop(self):
        pass

    def after_stop(self):
        pass

    # -----------------------

    def __init__(self, name=None, fn=None, **kwargs):
        if fn:
            self.run = fn
        self._current_executor = None
        self._active = False
        self._started = threading.Event()
        self._stopped = threading.Event()
        self.o = kwargs.get('o')
        self.on_error = kwargs.get('on_error')
        self.on_error_kwargs = kwargs.get('on_error_kwargs', {})
        self.supervisor = kwargs.get('supervisor', task_supervisor)
        self.poll_delay = kwargs.get('poll_delay', self.supervisor.poll_delay)
        self.set_name(name)
        self.start_stop_lock = threading.Lock()
        self._suppress_sleep = False
        self._executor_stop_event = threading.Event()
        self._is_worker = True
        self._run_with_worker_obj = True

    def set_name(self, name):
        self.name = '_background_worker_%s' % (name if name is not None else
                                               uuid.uuid4())

    def restart(self, *args, **kwargs):
        """
        Restart worker, all arguments will be passed to executor function as-is

        Args:
            wait: if True, wait until worker is stopped
        """
        self.stop(wait=kwargs.get('wait'))
        self.start(*args, **kwargs)

    def is_active(self):
        """
        Check if worker is active

        Returns:
            True if worker is active, otherwise False
        """
        return self._active

    def is_started(self):
        """
        Check if worker is started
        """
        return self._started.is_set()

    def is_stopped(self):
        """
        Check if worker is stopped
        """
        return self._stopped.is_set()

    def error(self):
        if self.on_error:
            self.on_error(**self.on_error_kwargs)
        else:
            raise

    def _send_executor_stop_event(self):
        self._current_executor = None
        self._executor_stop_event.set()

    def start(self, **kwargs):
        """
        Start worker, all arguments will be passed to executor function as-is
        """
        if self._active:
            return False
        self.start_stop_lock.acquire()
        try:
            self.before_start()
            self._active = True
            self._started.clear()
            self._stopped.clear()
            kw = kwargs.copy()
            if self._run_with_worker_obj:
                kw['_worker'] = self
            if not '_name' in kw:
                kw['_name'] = self.name
            if not 'o' in kw:
                kw['o'] = self.o
            self._executor_kwargs = kw
            self._start()
            self.after_start()
            return True
        finally:
            self.start_stop_lock.release()

    def _start(self):
        self.supervisor.spawn(self.loop)
        self._started.wait()
        self.supervisor.register_sync_scheduler(self)

    def _abort(self):
        self.mark_stopped()
        self.stop(wait=False)

    def process_result(self, result):
        pass

    def loop(self):
        self.mark_started()
        while self._active:
            try:
                if self.run(**self._executor_kwargs) is False:
                    return self._abort()
            except:
                self.error()
        self.mark_stopped()

    def mark_started(self):
        self._started.set()
        self._stopped.clear()
        if debug: logger.debug(self.name + ' started')

    def mark_stopped(self):
        self._stopped.set()
        self._started.clear()
        if debug: logger.debug(self.name + ' stopped')

    def stop(self, wait=True):
        """
        Stop worker
        """
        if self._active:
            self.start_stop_lock.acquire()
            try:
                self.before_stop()
                self._active = False
                self.send_stop_events()
                if wait:
                    self.wait_until_stop()
                self._stop(wait=wait)
                self.after_stop()
            finally:
                self.start_stop_lock.release()

    def _stop(self, **kwargs):
        self.supervisor.unregister_scheduler(self)

    def wait_until_stop(self):
        self._stopped.wait()


class BackgroundAsyncWorker(BackgroundWorker):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.worker_loop = kwargs.get('loop')
        self.aloop = None
        self._executor_is_async = False
        executor_pool = kwargs.get('pool')
        self._spawn = executor_pool.submit if \
                executor_pool else self.supervisor.spawn
        if isinstance(executor_pool, concurrent.futures.ProcessPoolExecutor):
            self._run_with_worker_obj = False

    def _register(self):
        if not self.worker_loop:
            raise RuntimeError(('{}: no loop defined').format(self.name))
        self.supervisor.register_async_scheduler(self)
        self._started.wait()

    def _start(self, *args, **kwargs):
        self.worker_loop = kwargs.get('_loop', self.worker_loop)
        if isinstance(self.worker_loop, str):
            self.worker_loop = self.supervisor.get_aloop(self.worker_loop)
        elif not self.worker_loop and self.supervisor.default_aloop:
            self.worker_loop = self.supervisor.default_aloop
        if isinstance(self.worker_loop, ALoop):
            self.aloop = self.worker_loop
            self.worker_loop = self.worker_loop.get_loop()
        self._executor_is_async = asyncio.iscoroutinefunction(self.run)
        self._register()

    def _stop(self, *args, **kwargs):
        self.supervisor.unregister_scheduler(self)

    def mark_started(self):
        self._executor_stop_event = asyncio.Event()
        super().mark_started()

    async def loop(self, *args, **kwargs):
        self.mark_started()
        while self._active:
            if self._current_executor:
                await self._executor_stop_event.wait()
                self._executor_stop_event.clear()
            if self._active:
                if not await self.launch_executor():
                    break
            else:
                break
            if not self._suppress_sleep:
                await asyncio.sleep(self.poll_delay)
        self.mark_stopped()

    def _run_callback(self, future):
        try:
            try:
                if future.result() is False:
                    self._abort()
            except:
                self.error()
        finally:
            self._send_executor_stop_event()

    def _send_executor_stop_event(self):
        asyncio.run_coroutine_threadsafe(self._set_stop_event(),
                                         loop=self.worker_loop)

    async def _set_stop_event(self):
        self._current_executor = None
        self._executor_stop_event.set()

    async def launch_executor(self, *args):
        if self._executor_is_async:
            try:
                result = await self.run(*args, **self._executor_kwargs)
            except:
                self.error()
                result = None
            if result is False: self._abort()
            return result is not False and self._active
        else:
            task = self._spawn(self.run, *args, **self._executor_kwargs)
            task.add_done_callback(self._run_callback)
            self._current_executor = task
            return task is not None and self._active


class BackgroundQueueWorker(BackgroundAsyncWorker):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        q = kwargs.get('q', kwargs.get('queue'))
        if isinstance(q, type):
            self._qclass = q
        else:
            self._qclass = asyncio.queues.Queue

    def put_threadsafe(self, t):
        asyncio.run_coroutine_threadsafe(self._Q.put(t), loop=self.worker_loop)

    async def put(self, t):
        await self._Q.put(t)

    def send_stop_events(self):
        try:
            self.put_threadsafe(None)
        except:
            pass

    def _stop(self, *args, **kwargs):
        super()._stop(*args, **kwargs)

    def before_queue_get(self):
        pass

    def after_queue_get(self, task):
        pass

    async def loop(self, *args, **kwargs):
        self._Q = self._qclass()
        self.mark_started()
        while self._active:
            self.before_queue_get()
            task = await self._Q.get()
            self.after_queue_get(task)
            try:
                if self._current_executor:
                    await self._executor_stop_event.wait()
                    self._executor_stop_event.clear()
                if self._active and task is not None:
                    if not await self.launch_executor(task):
                        break
                else:
                    break
                if not self._suppress_sleep:
                    await asyncio.sleep(self.poll_delay)
            finally:
                self._Q.task_done()
        self.mark_stopped()

    def get_queue_obj(self):
        return self._Q


class BackgroundEventWorker(BackgroundAsyncWorker):

    def trigger_threadsafe(self, force=False):
        if not self._current_executor or force:
            asyncio.run_coroutine_threadsafe(self._set_event(),
                                             loop=self.worker_loop)

    async def trigger(self, force=False):
        if not self._current_executor or force:
            await self._set_event()

    async def _set_event(self):
        self._E.set()

    async def loop(self, *args, **kwargs):
        self._E = asyncio.Event()
        self.mark_started()
        while self._active:
            if self._current_executor:
                await self._executor_stop_event.wait()
                self._executor_stop_event.clear()
            await self._E.wait()
            self._E.clear()
            if not self._active or not await self.launch_executor():
                break
            if not self._suppress_sleep:
                await asyncio.sleep(self.poll_delay)
        self.mark_stopped()

    def send_stop_events(self, *args, **kwargs):
        try:
            self.trigger_threadsafe(force=True)
        except:
            pass

    def get_event_obj(self):
        return self._E


class BackgroundIntervalWorker(BackgroundAsyncWorker):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.delay = kwargs.get(
            'interval', kwargs.get('delay', kwargs.get('delay_after', 1)))
        if 'interval' in kwargs:
            self.keep_interval = True
        else:
            self.keep_interval = False
        self._suppress_sleep = True
        self._sleep_task = None

    def _start(self, *args, **kwargs):
        self.delay = kwargs.get(
            '_interval',
            kwargs.get('_delay', kwargs.get('_delay_after', self.delay)))
        if '_interval' in kwargs:
            self.keep_interval = True
        super()._start(*args, **kwargs)
        return True

    def send_stop_events(self):
        try:
            self.trigger_threadsafe(force=True)
        except:
            pass

    def trigger_threadsafe(self, force=False):
        if not self._current_executor or force:
            asyncio.run_coroutine_threadsafe(self._cancel_sleep(),
                                             loop=self.worker_loop)

    async def trigger(self, force=False):
        if not self._current_executor or force:
            await self._cancel_sleep()

    async def _cancel_sleep(self):
        self._sleep_task.cancel()

    async def loop(self, *args, **kwargs):
        self.mark_started()
        if self.keep_interval:
            scheduled = time.perf_counter()
        while self._active:
            if self._current_executor:
                await self._executor_stop_event.wait()
                self._executor_stop_event.clear()
            if not self._active or not await self.launch_executor():
                break
            if not self.delay:
                tts = self.poll_delay
            elif self.keep_interval:
                scheduled += self.delay
                tts = scheduled - time.perf_counter()
            else:
                tts = self.delay
                if self._current_executor:
                    await self._executor_stop_event.wait()
                    self._executor_stop_event.clear()
            if tts > 0:
                self._sleep_task = asyncio.ensure_future(asyncio.sleep(tts))
                try:
                    await self._sleep_task
                except asyncio.CancelledError:
                    scheduled = time.perf_counter()
                    pass
                finally:
                    self._sleep_task = None
        self.mark_stopped()


def background_worker(*args, **kwargs):

    def decorator(f, **kw):
        func = f
        kw = kw.copy() if kw else kwargs
        if kwargs.get('q') or kwargs.get('queue'):
            C = BackgroundQueueWorker
        elif kwargs.get('e') or kwargs.get('event'):
            C = BackgroundEventWorker
        elif kwargs.get('i') or \
                kwargs.get('interval') or \
                kwargs.get('delay'):
            C = BackgroundIntervalWorker
        elif asyncio.iscoroutinefunction(func):
            C = BackgroundAsyncWorker
        else:
            C = BackgroundWorker
        if 'name' in kw:
            name = kw['name']
            del kw['name']
        else:
            name = func.__name__
        f = C(name=name, **kw)
        f.run = func
        return f

    return decorator if not args else decorator(args[0], **kwargs)
