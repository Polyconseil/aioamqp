Changelog
=========

Next release
------------

 * Add support for Python 3.9 and 3.10.
 * Drop support for Python 3.5 and 3.6.
 * Fix annoying auth method warning because of a wrong defined default argument (closes #214).
 * Support ``amqps://`` URLs.
 * Properly handle disabled heartbeats.

Aioamqp 0.14.0
--------------

 * Fix ``waiter already exist`` issue when creating multiple queues (closes #105).
 * Rename ``type`` to ``message_type`` in constant.Properties object to be full compatible with pamqp.
 * Add python 3.8 support.

Aioamqp 0.13.0
--------------

 * SSL Connections must be configured with an SSLContext object in ``connect`` and ``from_url`` (closes #142).
 * Uses pamqp to encode or decode protocol frames.
 * Drops support of python 3.3 and python 3.4.
 * Uses async and await keywords.
 * Fix pamqp `_frame_parts` call, now uses exposed `frame_parts`

Aioamqp 0.12.0
--------------

 * Fix an issue to use correct int encoder depending on int size (closes #180).
 * Call user-specified callback when a consumer is cancelled.

Aioamqp 0.11.0
--------------

 * Fix publish str payloads. Support will be removed in next major release.
 * Support for ``basic_return`` (closes #158).
 * Support for missings encoding and decoding types (closes #156).


Aioamqp 0.10.0
--------------

 * Remove ``timeout`` argument from all channel methods.
 * Clean up uses of ``no_wait`` argument from most channel methods.
 * Call ``drain()`` after sending every frame (or group of frames).
 * Make sure AmqpProtocol behaves identically on 3.4 and 3.5+ wrt EOF reception.

Aioamqp 0.9.0
-------------

 * Fix server cancel handling (closes #95).
 * Send "close ok" method on server-initiated close.
 * Validate internal state before trying to send messages.
 * Clarify which BSD license we actually use (3-clause).

Aioamqp 0.8.2
-------------

 * Really turn off heartbeat timers (closes #112).

Aioamqp 0.8.1
-------------

 * Turn off heartbeat timers when the connection is closed (closes #111).
 * Fix tests with python 3.5.2 (closes #107).
 * Properly handle unlimited sized payloads (closes #103).
 * API fixes in the documentation (closes #102, #110).
 * Add frame properties to returned value from ``basic_get()`` (closes #100).

Aioamqp 0.8.0
-------------

 * Complete overhaul of heartbeat (closes #96).
 * Prevent closing channels multiple times (inspired by PR #88).

Aioamqp 0.7.0
-------------

 * Add ``basic_client_nack`` and ``recover`` method (PR #72).
 * Sends ``server-close-ok`` in response to a ``server-close``.
 * Disable Nagle algorithm in ``connect`` (closes #70).
 * Handle ``CONNECTION_CLOSE`` during initial protocol handshake (closes #80).
 * Supports for python 3.5.
 * Few code refactors.
 * Dispatch does not catch ``KeyError`` anymore.

Aioamqp 0.6.0
-------------

 * The ``client_properties`` is now fully configurable.
 * Add more documentation.
 * Simplify the channel API: ``queue_name`` arg is no more required to declare
   a queue. ``basic_qos`` arguments are now optional.

Aioamqp 0.5.1
-------------

 * Fixes packaging issues when uploading to pypi.

Aioamqp 0.5.0
-------------

 * Add possibility to pass extra keyword arguments to protocol_factory when
   from_url is used to create a connection.
 * Add SSL support.
 * Support connection metadata customization, closes #40.
 * Remove the use of rabbitmqctl in tests.
 * Reduce the memory usage for channel recycling, closes #43.
 * Add the usage of a previously created eventloop, closes #56.
 * Removes the checks for coroutine callbacks, closes #55.
 * Connection tuning are now configurable.
 * Add a heartbeat method to know if the connection has fail, closes #3.
 * Change the callback signature. It now takes the channel as first parameter,
   closes: #47.


Aioamqp 0.4.0
-------------

 * Call the error callback on all circumtstances.

Aioamqp 0.3.0
-------------

 * The consume callback takes now 3 parameters: body, envelope, properties,
   closes #33.
 * Channel ids are now recycled, closes #36.

Aioamqp 0.2.1
-------------

 * connect returns a transport and protocol instance.

Aioamqp 0.2.0
-------------

 * Use a callback to consume messages.
