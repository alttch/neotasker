# neotasker
Lightweight Python library for modern thread / multiprocessing pooling and task
processing via asyncio.

<img src="https://img.shields.io/pypi/v/neotasker.svg" />
<img src="https://img.shields.io/badge/license-MIT-green.svg" />
<img src="https://img.shields.io/badge/python-3.5%20%7C%203.6%20%7C%203.7-blue.svg" />
<img src="https://img.shields.io/badge/-alpha-red.svg" />

Neotasker is lightweight variation of
[atasker](https://github.com/alttch/atasker) library: tasks don't have
priorities, go directly to ThreadPoolExecutor and are standard Python future
objects. This library is useful for the high-load projects with lightweight
tasks as majority tasks are directly proxied to pool.

Neotasker works on top of **ThreadPoolExecutor** and **asyncio** and provides
additional features:

* Easy thread pool and asyncio loops initialization
* Interval, queue and event-based workers
* Built-in integration with [aiosched](https://github.com/alttch/aiosched)

## Install

```bash
pip3 install neotasker
```

Sources: https://github.com/alttch/neotasker

Documentation: https://neotasker.readthedocs.io/

## Code examples

### Start/stop

```python

from neotasker import task_supervisor

# set pool size
# min_size='max' means pre-spawn all pool threads
task_supervisor.set_thread_pool(min_size='max', max_size=20)
task_supervisor.start()
# ...
# start workers, other threads etc.
# ...
# optionally block current thread
task_supervisor.block()

# stop from any thread
task_supervisor.stop()
```

### Executing future

You may work with *neotasker.thread_pool* object directly or use
*task_supervisor.spawn* function, which's directly mapped to
*thread_pool.submit*)

```python
from neotasker import thread_pool

thread_pool.start()

def mytask(a, b, c):
    print(f'I am working in the background! {a} {b} {c}')
    return 777

task = task_supervisor.spawn(mytask, 1, 2, c=3)

# get future result
result = task.result()
```
### Creating async io loop

```python
from neotasker import thread_pool

thread_pool.start()
task_supervisor.create_aloop('default', default=True)

# The loop will until supervisor is stopped
# Spawn coroutine from another thread:

task_supervisor.get_aloop().spawn_coroutine_threadsafe(coro)
```

### Worker examples

```python
from neotasker import background_worker, task_supervisor

task_supervisor.start()
# we need to create at least one aloop to start workers
task_supervisor.create_aloop('default', default=True)
# create one more async loop
task_supervisor.create_aloop('loop2')

@background_worker
def worker1(**kwargs):
    print('I am a simple background worker')

@background_worker
async def worker_async(**kwargs):
    print('I am async background worker')

@background_worker(interval=1, loop='loop2')
def worker2(**kwargs):
    print('I run every second!')

@background_worker(queue=True)
def worker3(task, **kwargs):
    print('I run when there is a task in my queue')

@background_worker(event=True)
def worker4(**kwargs):
    print('I run when triggered')

worker1.start()
worker_async.start()
worker2.start()
worker3.start()
worker4.start()

worker3.put_threadsafe('todo1')
worker4.trigger_threadsafe()

from neotasker import BackgroundIntervalWorker

class MyWorker(BackgroundIntervalWorker):

    def run(self, **kwargs):
        print('I am custom worker class')

worker5 = MyWorker(interval=0.1, name='worker5')
worker5.start()
```
