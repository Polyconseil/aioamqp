Examples
========

    Those examples are ported from the `RabbitMQ tutorial <http://www.rabbitmq.com/getstarted.html>`_. They are specific to `aioamqp` and uses `coroutines` exclusievely. Please read both documentation together, as the official documentation explain how to use the AMQP protocol correctly.

    Do not hesitate to use RabbitMQ `Shiny management interfaces <https://www.rabbitmq.com/management.html>`_, it really helps to understand which message is stored in which queues, and which consumer unqueues what queue.

    Using docker, you can run RabbitMQ using the following command line. Using this command line you will be able to run the examples and access the `RabbitMQ management interface <http://localhost:15672>`_ using the login `guest` and the password `guest`.

 .. code-block::shell
    docker run -d --log-driver=syslog -e RABBITMQ_NODENAME=my-rabbit --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management

Contents:

.. toctree::
   :maxdepth: 2

   hello_world
   work_queue
   publish_subscribe
   routing
   topics
   rpc
