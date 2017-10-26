import unittest
import asyncio

from . import testcase
from . import testing


class PublishTestCase(testcase.RabbitTestCase, unittest.TestCase):

    _multiprocess_can_split_ = True

    def assertOneMessage(self):
        for x in range(10):
            try:
                queues = self.list_queues()
                self.assertIn("q", queues)
                try:
                    self.assertEqual(1, queues["q"]['messages_ready_ram'])
                except (KeyError,AssertionError):
                    try:
                        self.assertEqual(1, queues["q"]['messages'])
                    except (KeyError,AssertionError):
                        self.assertEqual(1, queues["q"]['message_stats']['publish'])
            except Exception as exc:
                ex = exc
            else:
                break
            yield from asyncio.sleep(0.5, loop=self.loop)
        else:
            raise ex

    @testing.coroutine
    def test_publish(self):
        # declare
        yield from self.channel.queue_declare("q", exclusive=True, no_wait=False)
        yield from self.channel.exchange_declare("e", "fanout")
        yield from self.channel.queue_bind("q", "e", routing_key='')

        # publish
        yield from self.channel.publish("coucou", "e", routing_key='')

        self.assertOneMessage()

    @testing.coroutine
    def test_big_publish(self):
        # declare
        yield from self.channel.queue_declare("q", exclusive=True, no_wait=False)
        yield from self.channel.exchange_declare("e", "fanout")
        yield from self.channel.queue_bind("q", "e", routing_key='')

        # publish
        yield from self.channel.publish("a"*1000000, "e", routing_key='')

        self.assertOneMessage()

    @testing.coroutine
    def test_confirmed_publish(self):
        # declare
        yield from self.channel.confirm_select()
        self.assertTrue(self.channel.publisher_confirms)
        yield from self.channel.queue_declare("q", exclusive=True, no_wait=False)
        yield from self.channel.exchange_declare("e", "fanout")
        yield from self.channel.queue_bind("q", "e", routing_key='')

        # publish
        yield from self.channel.publish("coucou", "e", routing_key='')

        self.assertOneMessage()

