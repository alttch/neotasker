import threading
import multiprocessing
import time
import logging
import asyncio
import uuid

from concurrent.futures import (CancelledError, ThreadPoolExecutor, TimeoutError
                                as CFTimeoutError)
from aiosched import AsyncJobScheduler

# 3.6 compat
try:
    ACancelledError = asyncio.exceptions.CancelledError
except:
    ACancelledError = asyncio.CancelledError

debug = False

logger = logging.getLogger('neotasker')

thread_pool_default_size = multiprocessing.cpu_count() * 5
mp_pool_default_size = multiprocessing.cpu_count()

default_poll_delay = 0.01

# Python 3.6 compat
try:
    asyncio.all_tasks
except AttributeError:
    asyncio.all_tasks = asyncio.Task.all_tasks


class ALoop:

    def __init__(self, name=None, supervisor=None):
        self.name = name if name else str(uuid.uuid4())
        self._active = False
        self.daemon = False
        self.poll_delay = default_poll_delay
        self.thread = None
        self.supervisor = supervisor
        self._started = threading.Event()

    def run_coroutine_threadsafe(self, coro):
        if not self._active:
            raise RuntimeError('{} aloop {} is not active'.format(
                self.supervisor.id, self.name))
        future = asyncio.run_coroutine_threadsafe(coro, loop=self._loop)
        return future.result()

    def spawn_coroutine_threadsafe(self, coro):
        if not self._active:
            raise RuntimeError('{} aloop {} is not active'.format(
                self.supervisor.id, self.name))
        return asyncio.run_coroutine_threadsafe(coro, loop=self.loop)

    def start(self):
        if not self._active:
            self._started.clear()
            t = threading.Thread(name='supervisor_{}_aloop_{}'.format(
                self.supervisor.id, self.name),
                                 target=self._start_loop)
            t.setDaemon(self.daemon)
            t.start()
            self._started.wait()

    def get_loop(self):
        return None if not self._active else self._loop

    def _start_loop(self):
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._aio_loop())
        except (CancelledError, ACancelledError):
            logger.warning('supervisor {} aloop {} had active tasks'.format(
                self.supervisor.id, self.name))

    async def _aio_loop(self):
        self._stop_event = asyncio.Event()
        self.thread = threading.current_thread()
        self._active = True
        logger.info('supervisor {} aloop {} started'.format(
            self.supervisor.id, self.name))
        self._started.set()
        await self._stop_event.wait()
        logger.info('supervisor {} aloop {} finished'.format(
            self.supervisor.id, self.name))

    def _cancel_all_tasks(self):
        for task in asyncio.all_tasks(loop=self._loop):
            task.cancel()

    async def _set_stop_event(self):
        self._stop_event.set()

    def stop(self, wait=True, cancel_tasks=False):
        if self._active:
            if cancel_tasks:
                self._cancel_all_tasks()
                if debug:
                    logger.debug(
                        'supervisor {} aloop {} remaining tasks canceled'.
                        format(self.supervisor.id, self.name))
            if isinstance(wait, bool):
                to_wait = None
            else:
                to_wait = time.perf_counter() + wait
            self._active = False
            asyncio.run_coroutine_threadsafe(self._set_stop_event(),
                                             loop=self._loop)
            while True:
                if to_wait and time.perf_counter() > to_wait:
                    logger.warning(
                        ('supervisor {} aloop {} wait timeout, ' +
                         'canceling all tasks').format(self.supervisor.id,
                                                       self.name))
                    self._cancel_all_tasks()
                    break
                else:
                    can_break = True
                    for t in asyncio.all_tasks(self._loop):
                        if not t.cancelled() and not t.done():
                            can_break = False
                            break
                    if can_break:
                        break
                time.sleep(self.poll_delay)
        if wait and self.thread:
            self.thread.join()

    @property
    def active(self):
        return self._active

    @property
    def loop(self):
        return self._loop


class TaskSupervisor:

    def __init__(self, supervisor_id=None):

        self.poll_delay = default_poll_delay

        self.id = supervisor_id if supervisor_id else str(uuid.uuid4())

        self._lock = threading.Lock()

        self._schedulers = {}

        self.default_aloop = None
        self.default_async_job_scheduler = None

        self.aloops = {}
        self.async_job_schedulers = {}

        self.set_thread_pool()

        self._active = False

        self.pool_timeout = 1

        self.pool_check_interval = 5

    def set_thread_pool(self, min_size=None, max_size=None, pool=None):
        if min_size is None:
            min_size = 0
        if max_size is None:
            max_size = thread_pool_default_size
        if pool is not None:
            self.thread_pool = pool
        else:
            if min_size == 'max':
                min_size = max_size
            elif max_size < min_size:
                raise ValueError(
                    'min pool size ({}) can not be larger than max ({})'.format(
                        min_size, max_size))
            self.thread_pool = ThreadPoolExecutor(
                max_workers=max_size,
                thread_name_prefix='supervisor_{}_pool'.format(self.id))
        self.spawn = self.thread_pool.submit
        self._thread_pool_min_size = min_size
        self._thread_pool_max_size = max_size

    def create_aloop(self, name, daemon=False, start=True, default=False):
        if name == '__supervisor__':
            raise RuntimeError('Name "__supervisor__" is reserved')
        with self._lock:
            if name in self.aloops:
                logger.error('supervisor {} loop {} already exists'.format(
                    self.id, name))
                return False
        l = ALoop(name, supervisor=self)
        l.daemon = daemon
        l.poll_delay = self.poll_delay
        with self._lock:
            self.aloops[name] = l
            if start:
                l.start()
            if default:
                self.set_default_aloop(l)
        return l

    def create_async_job_scheduler(self,
                                   name,
                                   aloop=None,
                                   start=True,
                                   default=False):
        """
        Create async job scheduler (aiosched.scheduler)

        ALoop must always be specified or default ALoop defined
        """
        if name == '__supervisor__':
            raise RuntimeError('Name "__supervisor__" is reserved')
        with self._lock:
            if name in self.async_job_schedulers:
                logger.error(
                    'supervisor {} async job_scheduler {} already exists'.
                    format(self.id, name))
                return False
        l = AsyncJobScheduler(name)
        if aloop is None:
            aloop = self.default_aloop
        elif not isinstance(aloop, ALoop):
            aloop = self.get_aloop(aloop)
        loop = aloop.get_loop()
        with self._lock:
            self.async_job_schedulers[name] = l
            if default:
                self.set_default_async_job_scheduler(l)
        if start:
            l.set_loop(loop)
            l._aloop = aloop
            aloop.spawn_coroutine_threadsafe(l.scheduler_loop())
        else:
            l.set_loop(loop)
        return l

    def create_async_job(self, scheduler=None, **kwargs):
        if scheduler is None:
            scheduler = self.default_async_job_scheduler
        elif isinstance(scheduler, str):
            scheduler = self.async_job_schedulers[scheduler]
        return scheduler.create_threadsafe(**kwargs)

    def set_default_aloop(self, aloop):
        self.default_aloop = aloop

    def set_default_async_job_scheduler(self, scheduler):
        self.default_async_job_scheduler = scheduler

    def get_aloop(self, name=None, default=True):
        with self._lock:
            if name is not None:
                return self.aloops.get(name)
            elif default:
                return self.default_aloop

    def start_aloop(self, name):
        with self._lock:
            if name not in self.aloops:
                logger.error('supervisor {} loop {} not found'.format(
                    self.id, name))
                return False
            else:
                self.aloops[name].start()
                return True

    def stop_aloop(self, name, wait=True, cancel_tasks=False, _lock=True):
        if _lock:
            self._lock.acquire()
        try:
            if name not in self.aloops:
                logger.error('supervisor {} loop {} not found'.format(
                    self.id, name))
                return False
            else:
                self.aloops[name].stop(wait=wait, cancel_tasks=cancel_tasks)
                return True
        finally:
            if _lock:
                self._lock.release()

    def get_info(self, aloops=True, schedulers=True, async_job_schedulers=True):

        class SupervisorInfo:
            pass

        result = SupervisorInfo()
        with self._lock:
            result.id = self.id
            result.active = self._active
            if aloops:
                result.aloops = self.aloops.copy()
            if schedulers:
                result.schedulers = self._schedulers.copy()
            if async_job_schedulers:
                result.async_job_schedulers = self.async_job_schedulers.copy()
        return result

    def get_aloops(self):
        with self._lock:
            return self.aloops.copy()

    def get_schedulers(self):
        with self._lock:
            return self._schedulers.copy()

    def start(self):

        def _prespawn():
            time.sleep(0.2)

        for i in range(self._thread_pool_min_size):
            self.thread_pool.submit(_prespawn)
        self._active = True
        threading.Thread(target=self._monitor,
                         daemon=True,
                         name='supervisor_{}_pool_monitor'.format(
                             self.id)).start()
        logger.info('supervisor {} started, executor pool: {}/{}'.format(
            self.id, self._thread_pool_min_size, self._thread_pool_max_size))

    def _monitor(self):

        def _test():
            return True

        while self._active:
            time.sleep(self.pool_check_interval)
            if self._active:
                future = self.spawn(_test)
                try:
                    result = future.result(timeout=self.pool_timeout)
                    if result is not True:
                        logger.critical('{} pool error'.format(self.id))
                    else:
                        if debug:
                            logger.debug('{} pool health check OK'.format(
                                self.id))
                except CFTimeoutError:
                    logger.critical(
                        '{} pool timeout. Consider increasing thread pool size'.
                        format(self.id))
                except:
                    if debug:
                        import traceback
                        logger.debug(traceback.format_exc())
            else:
                break

    def block(self):
        while self._active:
            time.sleep(0.1)

    def _stop_async_job_schedulers(self, wait=True):
        with self._lock:
            schedulers = self.async_job_schedulers.copy().items()
        for n, s in schedulers:
            try:
                s.stop(wait=wait)
            except:
                raise

    def _stop_schedulers(self, wait=True):
        with self._lock:
            schedulers = self._schedulers.copy()
        for n, s in schedulers.items():
            s.stop(wait=wait)

    def stop(self, wait=True, stop_schedulers=True, cancel_tasks=False):
        self._active = False
        if isinstance(wait, bool):
            to_wait = None
        else:
            to_wait = time.perf_counter() + wait
        if stop_schedulers:
            self._stop_async_job_schedulers(
                wait=to_wait - time.perf_counter() if to_wait else wait)
            self._stop_schedulers(True if wait else False)
            if debug:
                logger.debug('supervisor {} schedulers stopped'.format(self.id))
        with self._lock:
            for i, l in self.aloops.items():
                self.stop_aloop(i,
                                wait=to_wait -
                                time.perf_counter() if to_wait else wait,
                                cancel_tasks=cancel_tasks,
                                _lock=False)
            if debug:
                logger.debug('supervisor {} async loops stopped'.format(
                    self.id))
        self.thread_pool.shutdown()
        logger.info('supervisor {} stopped'.format(self.id))

    def register_sync_scheduler(self, scheduler):
        with self._lock:
            self._schedulers[scheduler._name] = scheduler
        return True

    def register_async_scheduler(self, scheduler):
        with self._lock:
            self._schedulers[scheduler._name] = scheduler
            asyncio.run_coroutine_threadsafe(scheduler.loop(),
                                             loop=scheduler.worker_loop)
        return True

    def unregister_scheduler(self, scheduler):
        with self._lock:
            try:
                del self._schedulers[scheduler._name]
                return True
            except:
                return False

    @property
    def active(self):
        return self._active
