Thread local proxy
******************

.. code:: python

    from neotasker import g

    if not g.has('db'):
        g.set('db', <new_db_connection>)

Supports methods:

.. automodule:: neotasker
.. autoclass:: LocalProxy
    :members:
