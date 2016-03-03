"""
    Compatibility between python or package versions
"""

import asyncio

try:
    from asyncio import ensure_future
except ImportError:
    ensure_future = asyncio.async
