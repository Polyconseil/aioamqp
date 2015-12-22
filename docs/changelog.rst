Changelog
=========


Aioamqp 0.6.0
-------------

 * The `client_properties` is now fully configurable.
 * Add more documentation.
 * Simplify the channel API: `queue_name` arg is no more required to declare a queue. `basic_qos` arguments are now optional.

Aioamqp 0.5.1
-------------

 * Fixes packaging issues when uploading to pypi.

Aioamqp 0.5.0
-------------

 * Add possibility to pass extra keyword arguments to protocol_factory when from_url is used to create a connection.
 * Add SSL support.
 * Support connection metadata customization, closes #40.
 * Remove the use of rabbitmqctl in tests.
 * Reduce the memory usage for channel recycling, closes #43.
 * Add the usage of a previously created eventloop, closes #56.
 * Removes the checks for coroutine callbacks, closes #55.
 * Connection tuning are now configurable.
 * Add a heartbeat method to know if the connection has fail, closes #3.
 * Change the callback signature. It now takes the channel as first parameter, closes: #47.


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
