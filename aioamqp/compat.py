"""
    Compatibility between python or package versions
"""
# pylint: disable=unused-import

import asyncio

try:
    from asyncio import ensure_future
except ImportError:
    ensure_future = asyncio.async


def iscoroutinepartial(fn):
    # http://bugs.python.org/issue23519

    while True:
        parent = fn

        fn = getattr(parent, 'func', None)

        if fn is None:
            break

    return asyncio.iscoroutinefunction(parent)
