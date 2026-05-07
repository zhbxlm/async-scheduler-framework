from __future__ import annotations

import inspect


async def maybe_await(value):
    """Await value if it is a coroutine, otherwise return as-is.

    Used to support both sync and async SQLAlchemy session implementations
    in service layer code.
    """
    if inspect.isawaitable(value):
        return await value
    return value
