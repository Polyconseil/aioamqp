Changelog
=========

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
