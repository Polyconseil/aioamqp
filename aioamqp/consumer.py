# -*- coding: utf-8 -*-
import asyncio

import sys

PY35 = sys.version_info >= (3, 5)


class Consumer:
    def __init__(self, queue: asyncio.Queue, consumer_tag):
        self.queue = queue
        self.tag = consumer_tag
        self.message = None

    if PY35:
        async def __aiter__(self):
            return self

        async def __anext__(self):
            return self.fetch_message()

    @asyncio.coroutine
    def fetch_message(self):

        self.message = yield from self.queue.get()
        if self.message:
            return self.message
        else:
            raise StopIteration()

    def get_message(self):
        return self.message