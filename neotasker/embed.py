"""
Embedded Python helper module
"""
import sys
import asyncio
import traceback
import logging
from neotasker import task_supervisor

from types import SimpleNamespace

_d = SimpleNamespace(loop=None)

debug = False


def set_poll_delay(poll_delay):
    """
    Sets neotasker default supervisor poll delay
    """
    task_supervisor.poll_delay = poll_delay


def set_thread_pool_size(size):
    """
    Sets neotasker default supervisor thread pool size

    Args:
        size: tuple (min, max)
    """
    task_supervisor.set_thread_pool(min_size=size[0], max_size=size[1])


def add_import_path(import_path):
    """
    Extends Python import path

    Args:
        import_path: path or list of paths to extend sys.path with
    """
    if isinstance(import_path, list):
        sys.path = import_path + sys.path
    else:
        sys.path.insert(0, import_path)


def start():
    """
    Starts neotasker, creates "eapi" asyncio loop
    """
    _d.loop = task_supervisor.create_aloop('eapi').get_loop()
    task_supervisor.start()
    if debug:
        logging.debug('neotasker embed started')


def stop(wait=True):
    """
    Stops neotasker

    Args:
        wait: wait until all tasks are finished
    """
    task_supervisor.stop(wait=wait)


def spawn(func, *args, **kwargs):
    """
    Spawns task in thread pool, does not return the result
    """
    if debug:
        logging.debug(
            f'spawning task: {func}\nargs: {args}\nkwargs: {kwargs}\n' +
            '-' * 40)
    task_supervisor.spawn(func, *args, **kwargs)


def call(call_id, func, *args, **kwargs):
    """
    Spawns task in thread pool and reports the result

    Runs call_async() in "eapi" asyncio loop

    Args:
        call_id: custom identifier used by callback to report the result
    """
    if debug:
        logging.debug(f'(sync) calling task {call_id}: {func}')
    asyncio.run_coroutine_threadsafe(call_async(call_id, func, *args, **kwargs),
                                     loop=_d.loop)


async def call_async(call_id, func, *args, **kwargs):
    """
    Spawns task in thread pool and reports the result (async)

    Args:
        call_id: custom identifier used by callback to report the result
    """
    if debug:
        logging.debug(
            f'calling task {call_id}: {func}\nargs: {args}\nkwargs: {kwargs}\n'
            + '-' * 40)
    try:
        result = await _d.loop.run_in_executor(task_supervisor.thread_pool,
                                               func, *args, **kwargs)
        if debug:
            logging.debug(f'task {call_id} result:\n{result}\n' + '-' * 40)
        report_result(call_id, result=result)
    except Exception as e:
        if debug:
            logging.debug(
                f'task {call_id} exception {e.__class__.__name__}: {e}'
                f'\n{traceback.format_exc()}\n' + '-' * 40)
        report_result(call_id,
                      error=(e.__class__.__name__, str(e),
                             traceback.format_exc()))


def report_result(call_id, result=None, error=None):
    """
    Called when a result is reported by call or call_async

    This function MUST be overriden by the super process to get call results

    Args:
        call_id: custom identifier
        result: task result, as-is
        error: filled if task raised an exception, as tuple (exception class
               name, exception message, traceback)
    """
    print('CRITICAL: Function report_result MUST be overriden!',
          file=sys.stderr)
    print(f'Task {call_id} completed', file=sys.stderr)
    if result:
        print('Result:', file=sys.stderr)
        print('-' * 40, file=sys.stderr)
        print(result, file=sys.stderr)
    elif error:
        print(f'Exception {error[0]}: {error[1]}', file=sys.stderr)
        print(error[2], file=sys.stderr)
    print('-' * 40, file=sys.stderr, flush=True)


def set_debug(mode=True):
    """
    Link to neotasker.set_debug
    """
    from neotasker import set_debug as _set_debug
    _set_debug(mode)
