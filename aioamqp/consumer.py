# -*- coding: utf-8 -*-
import asyncio

import sys

import logging

PY35 = sys.version_info >= (3, 5)
logger = logging.getLogger(__name__)


class ConsumerStoped(Exception):
    pass


class Consumer:
    def __init__(self, queue: asyncio.Queue, consumer_tag):
        self._queue = queue
        self.tag = consumer_tag
        self.message = None
        self._stoped = False

    if PY35:
        @asyncio.coroutine
        def __aiter__(self):
            return self

        @asyncio.coroutine
        def __anext__(self):
            if not self._stoped:
                self.message = yield from self._queue.get()
                if isinstance(self.message, StopIteration):
                    self._stoped = True
                    raise StopAsyncIteration()
                else:
                    return self.message
            raise StopAsyncIteration()

    @asyncio.coroutine
    def fetch_message(self):
        if not self._stoped:
            self.message = yield from self._queue.get()
            if isinstance(self.message, StopIteration):
                self._stoped = True
                return False
            else:
                return True
        else:
            return False

    def get_message(self):
        if self._stoped:
            raise ConsumerStoped()
        return self.message
