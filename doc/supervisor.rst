Task supervisor
***************

Task supervisor is a component which manages task thread pool and run task
:doc:`schedulers (workers)<workers>`.

.. contents::

Usage
=====

When **neotasker** package is imported, default task supervisor is
automatically created.

.. code:: python

    from neotasker import task_supervisor

    # thread pool
    task_supervisor.set_thread_pool(max_size=20)
    task_supervisor.start()

.. warning::

    Task supervisor must be started before any scheduler/worker or task.

Pool size
=========

Parameters *min_size* and *max_size* set actual system thread pool size. If
*max_size* is not specified, it's set to *pool_size + reserve_normal +
reserve_high*. It's recommended to set *max_size* slightly larger manually to
have a space for critical tasks.

By default, *max_size* is CPU count * 5. You may use argument *min_size='max'*
to automatically set minimal pool size to max.

.. note::

    pool size can be changed while task supervisor is running.

Poll delay
==========

Poll delay is a delay (in seconds), which is used only by event and queue
:doc:`workers<workers>` to prevent asyncio loop spamming and some other methods
like *start/stop*.

Lower poll delay = higher CPU usage, higher poll delay = lower reaction time.

Default poll delay is 0.01 second. Can be changed with:

.. code:: python

    task_supervisor.poll_delay = 0.1 # set poll delay to 100ms

Blocking
========

Task supervisor is started in its own thread. If you want to block current
thread, you may use method

.. code:: python

    task_supervisor.block()

which will just sleep while task supervisor is active.

Stopping task supervisor
========================

.. code:: python

    task_supervisor.stop(wait=True, stop_schedulers=True, cancel_tasks=False)

Params:

* **wait** wait until tasks and scheduler coroutines finish. If
  **wait=<number>**, task supervisor will wait until coroutines finish for the
  max. *wait* seconds. However if requested to stop schedulers (workers) or
  task threads are currently running, method *stop* wait until they finish for
  the unlimited time.

* **stop_schedulers** before stopping the main event loop, task scheduler will
  call *stop* method of all schedulers running.

* **cancel_tasks** if specified, task supervisor will try to forcibly cancel
  all scheduler coroutines. 

.. _aloops:

aloops: async targets and tasks
===============================

Usually it's unsafe to run both :doc:`schedulers (workers)<workers>` target
functions and custom tasks in supervisor's event loop. Workers use event loop
by default and if anything is blocked, the program may be freezed.

To avoid this, it's strongly recommended to create independent async loops for
your custom tasks. **neotasker** supervisor has built-in engine for async
loops, called "aloops", each aloop run in a separated thread and doesn't
interfere with supervisor event loop and others.

Create
------

If you plan to use async worker target functions, create aloop:

.. code:: python

   a = task_supervisor.create_aloop('myworkers', default=True, daemon=True)
   # the loop is instantly started by default, to prevent add param start=False
   # and then use
   # task_supervisor.start_aloop('myworkers')

To determine in which thread target function is started, simply get its name.
aloop threads are called "supervisor_aloop_<name>".

Using with workers
------------------

Workers automatically launch async target function in default aloop, or aloop
can be specified with *loop=* at init or *_loop=* at startup.

Executing own coroutines
------------------------

aloops have 2 methods to execute own coroutines:

.. code:: python

   # put coroutine to loop
   task = aloop.spawn_coroutine_threadsafe(coro)

   # blocking wait for result from coroutine
   result = aloop.run_coroutine_threadsafe(coro)

Other supervisor methods
------------------------

.. note::

   It's not recommended to create/start/stop aloops without supervisor

.. code:: python

   # set default aloop
   task_supervisor.set_default_aloop(aloop):

   # get aloop by name
   task_supervisor.get_aloop(name)

   # stop aloop (not required, supervisor stops all aloops at shutdown)
   task_supervisor.stop_aloop(name)

   # get aloop async event loop object for direct access
   aloop.get_loop()

.. _create_mp_pool:

Multiprocessing
===============

It's possible to replace *task_supervisor.thread_pool* with
*ProcessPoolExecutor* object and majority functions will continue working
properly.

Custom task supervisor
======================

.. code:: python

   from neotasker import TaskSupervisor

   # ID is used only for logging
   my_supervisor = TaskSupervisor(id='my_supervisor')

   class MyTaskSupervisor(TaskSupervisor):
      # .......

   # if ID is not set, random UUID will be assigned
   my_supervisor2 = MyTaskSupervisor()

Putting own tasks
=================

Task supervisor method *spawn* is mapped directly to *thread_pool.submit* and
returns standard future object.

You may also access *task_supervisor.thead_pool* directly.

Creating own schedulers
=======================

Own task scheduler (worker) can be registered in task supervisor with:

.. code:: python

    task_supervisor.register_scheduler(scheduler)

Where *scheduler* = scheduler object, which should implement at least *stop*
(regular) and *loop* (async) methods.

Task supervisor can also register synchronous schedulers/workers, but it can
only stop them when *stop* method is called:

.. code:: python

    task_supervisor.register_sync_scheduler(scheduler)

To unregister schedulers from task supervisor, use *unregister_scheduler* and
*unregister_sync_scheduler* methods.
