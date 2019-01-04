import asynctest
import asyncio

from . import testcase


class PublishTestCase(testcase.RabbitTestCaseMixin, asynctest.TestCase):

    _multiprocess_can_split_ = True

    async def test_publish(self):
        # declare
        await self.channel.queue_declare("q", exclusive=True, no_wait=False)
        await self.channel.exchange_declare("e", "fanout")
        await self.channel.queue_bind("q", "e", routing_key='')

        # publish
        await self.channel.publish("coucou", "e", routing_key='')

        queues = self.list_queues()
        self.assertIn("q", queues)
        self.assertEqual(1, queues["q"]['messages'])

    async def test_empty_publish(self):
        # declare
        await self.channel.queue_declare("q", exclusive=True, no_wait=False)
        await self.channel.exchange_declare("e", "fanout")
        await self.channel.queue_bind("q", "e", routing_key='')

        # publish
        await self.channel.publish("", "e", routing_key='')

        queues = self.list_queues()
        self.assertIn("q", queues)
        self.assertEqual(1, queues["q"]["messages"])
        self.assertEqual(0, queues["q"]["message_bytes"])

    async def test_big_publish(self):
        # declare
        await self.channel.queue_declare("q", exclusive=True, no_wait=False)
        await self.channel.exchange_declare("e", "fanout")
        await self.channel.queue_bind("q", "e", routing_key='')

        # publish
        await self.channel.publish("a"*1000000, "e", routing_key='')

        queues = self.list_queues()
        self.assertIn("q", queues)
        self.assertEqual(1, queues["q"]['messages'])

    async def test_big_unicode_publish(self):
        # declare
        await self.channel.queue_declare("q", exclusive=True, no_wait=False)
        await self.channel.exchange_declare("e", "fanout")
        await self.channel.queue_bind("q", "e", routing_key='')

        # publish
        await self.channel.publish("Ы"*1000000, "e", routing_key='')
        await self.channel.publish("Ы"*1000000, "e", routing_key='')

        queues = self.list_queues()
        self.assertIn("q", queues)
        self.assertEqual(2, queues["q"]['messages'])

    async def test_confirmed_publish(self):
        # declare
        await self.channel.confirm_select()
        self.assertTrue(self.channel.publisher_confirms)
        await self.channel.queue_declare("q", exclusive=True, no_wait=False)
        await self.channel.exchange_declare("e", "fanout")
        await self.channel.queue_bind("q", "e", routing_key='')

        # publish
        await self.channel.publish("coucou", "e", routing_key='')

        queues = self.list_queues()
        self.assertIn("q", queues)
        self.assertEqual(1, queues["q"]['messages'])

    async def test_return_from_publish(self):
        called = False

        async def callback(channel, body, envelope, properties):
            nonlocal called
            called = True
        channel = await self.amqp.channel(return_callback=callback)

        # declare
        await channel.exchange_declare("e", "topic")

        # publish
        await channel.publish("coucou", "e", routing_key="not.found",
                                   mandatory=True)

        for _i in range(10):
            if called:
                break
            await asyncio.sleep(0.1)

        self.assertTrue(called)
