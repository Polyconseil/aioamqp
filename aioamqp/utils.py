import asyncio

def iscoroutinefunction(func):
    # Workaround for curried and partial functions.
    if asyncio.iscoroutinefunction(func):
        return True

    return asyncio.iscoroutinefunction(getattr(func, 'func', None))
