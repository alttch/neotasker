Task collections
****************

Task collections are useful when you need to run a pack of tasks e.g. on
program startup or shutdown. Currently collections support running task
functions only either in a foreground (one-by-one) or as the threads.

Function priority must be specified as a number (lower = higher priority).

.. automodule:: neotasker

FunctionCollection
==================

Simple collection of functions.

.. code:: python

    from neotasker import FunctionCollection

    def error(**kwargs):
       import traceback
       traceback.print_exc()

    startup = FunctionCollection(on_error=error)

    @startup
    def f1():
        return 1

    @startup(priority=100)
    def f2():
        return 2

    # lower number = higher priority
    @startup(priority=10)
    def f3():
        return 3

    result, all_ok = startup.execute()

.. autoclass:: FunctionCollection
    :members:
