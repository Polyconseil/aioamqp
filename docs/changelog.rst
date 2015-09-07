Changelog
=========


Next version (not yet released)
-------------------------------

 * Add possibility to pass extra keyword arguments to protocol_factory when from_url is used to create a connection
 * Add SSL support.
 * Support connection metadata customization, closes #40.
 * Remove the use of rabbitmqctl in tests.
 * Reduce the memory usage for channel recycling, close #43.


Aioamqp 0.4.0
-------------

 * Call the error callback on all circumtstances.

Aioamqp 0.3.0
-------------

 * The consume callback takes now 3 parameters: body, envelope, properties, closes #33.
 * Channel ids are now recycled, closes #36.

Aioamqp 0.2.1
-------------

 * connect returns a transport and protocol instance.

Aioamqp 0.2.0
-------------

 * Use a callback to consume messages.
