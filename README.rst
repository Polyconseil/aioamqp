aioamqp
=======

.. image:: https://badge.fury.io/py/aioamqp.svg
    :target: http://badge.fury.io/py/aioamqp
.. image:: https://travis-ci.org/Polyconseil/aioamqp.svg?branch=master
    :target: https://travis-ci.org/Polyconseil/aioamqp


``aioamqp`` library is a pure-Python implementation of the `AMQP 0.9.1 protocol`_.

Built on top on Python's asynchronous I/O support introduced in `PEP 3156`_, it provides an API based on coroutines, making it easy to write highly concurrent applications.

Bug reports, patches and suggestions welcome! Just open an issue_ or send a `pull request`_.

tests
-----

Tests require an instance of RabbitMQ. You can start a new instance using docker::

     docker run -v -d --log-driver=syslog -e RABBITMQ_NODENAME=my-rabbit --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management

A simple `make test` would run nose.


.. _AMQP 0.9.1 protocol: https://www.rabbitmq.com/amqp-0-9-1-quickref.html
.. _PEP 3156: http://www.python.org/dev/peps/pep-3156/
.. _issue: https://github.com/Polyconseil/aioamqp/issues/new
.. _pull request: https://github.com/Polyconseil/aioamqp/compare/
