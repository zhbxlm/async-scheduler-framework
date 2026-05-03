from __future__ import annotations

import asyncio
import os
from urllib.parse import urlparse, urlunparse

import pytest

from async_scheduler.settings import get_settings
import pytest_asyncio


def _candidate_db_url() -> str:
    return os.environ.get(
        "TEST_DATABASE_URL",
        os.environ.get(
            "DATABASE_URL",
            "mysql+asyncmy://async_scheduler:async_scheduler@127.0.0.1:3306/async_scheduler",
        ),
    )


def _mysql_reachable(url: str) -> bool:
    parsed = urlparse(url)
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or 3306

    async def _probe() -> bool:
        try:
            reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=1.0)
            writer.close()
            await writer.wait_closed()
            return True
        except Exception:
            return False

    try:
        return asyncio.run(_probe())
    except RuntimeError:
        return False


def _redis_reachable() -> bool:
    import socket
    s = socket.socket()
    s.settimeout(1.0)
    try:
        s.connect((os.environ.get('TEST_REDIS_HOST', '127.0.0.1'), int(os.environ.get('TEST_REDIS_PORT', '6379'))))
        return True
    except Exception:
        return False
    finally:
        s.close()


def pytest_configure(config):
    config.addinivalue_line("markers", "mysql_required: mark test as requiring reachable MySQL")
    config.addinivalue_line("markers", "redis_required: mark test as requiring reachable Redis")


def pytest_sessionstart(session):
    # Ensure cached settings are re-read inside pytest.
    get_settings.cache_clear()
    try:
        import async_scheduler.persistence.database as db
        db._engine = None
        db._session_factory = None
        db._bound_url = None
    except Exception:
        pass


def _derive_test_db_url(nodeid: str) -> str:
    base = _candidate_db_url()
    parsed = urlparse(base)
    db_name = (parsed.path or '/async_scheduler_test').lstrip('/')
    safe = nodeid.split('::', 1)[0].replace('/', '_').replace('.', '_').replace('-', '_')
    derived_name = f"{db_name}_{safe}"
    return urlunparse(parsed._replace(path='/' + derived_name))


async def _ensure_database_exists(url: str) -> None:
    parsed = urlparse(url)
    db_name = (parsed.path or '/async_scheduler_test').lstrip('/')
    root_url = urlunparse(parsed._replace(path='/mysql', netloc=f"root@{parsed.hostname}:{parsed.port or 3306}"))
    try:
        import asyncmy
        conn = await asyncmy.connect(host=parsed.hostname or '127.0.0.1', port=parsed.port or 3306, user='root', password='', autocommit=True)
        async with conn.cursor() as cur:
            await cur.execute(f"CREATE DATABASE IF NOT EXISTS `{db_name}`")
            await cur.execute(f"GRANT ALL PRIVILEGES ON `{db_name}`.* TO 'async_scheduler'@'localhost'")
            await cur.execute(f"GRANT ALL PRIVILEGES ON `{db_name}`.* TO 'async_scheduler'@'127.0.0.1'")
            await cur.execute("FLUSH PRIVILEGES")
        conn.close()
    except Exception:
        pass


@pytest_asyncio.fixture(autouse=True)
async def _reset_db_engine_between_tests(request):
    original_test_db_url = os.environ.get('TEST_DATABASE_URL')
    os.environ['TEST_DATABASE_URL'] = _derive_test_db_url(request.node.nodeid)
    get_settings.cache_clear()
    await _ensure_database_exists(os.environ['TEST_DATABASE_URL'])
    try:
        from redis.asyncio import Redis
        redis = Redis.from_url(f"redis://{os.environ.get('TEST_REDIS_HOST', '127.0.0.1')}:{os.environ.get('TEST_REDIS_PORT', '6379')}/0", decode_responses=True)
        await redis.flushdb()
        await redis.aclose()
    except Exception:
        pass
    try:
        import async_scheduler.persistence.database as db
        db._engine = None
        db._session_factory = None
        db._bound_url = None
    except Exception:
        pass
    yield
    try:
        from async_scheduler.persistence import close_db
        await close_db()
    except Exception:
        pass
    if original_test_db_url is None:
        os.environ.pop('TEST_DATABASE_URL', None)
    else:
        os.environ['TEST_DATABASE_URL'] = original_test_db_url
    get_settings.cache_clear()


def pytest_collection_modifyitems(config, items):
    db_url = _candidate_db_url()
    mysql_ok = db_url.startswith("mysql") and _mysql_reachable(db_url)
    redis_ok = _redis_reachable()
    skip_mysql = pytest.mark.skip(reason="MySQL not reachable; set TEST_DATABASE_URL or start local MySQL")
    skip_redis = pytest.mark.skip(reason="Redis not reachable; set TEST_REDIS_HOST/PORT or start local Redis")

    for item in items:
        if "mysql_required" in item.keywords and not mysql_ok:
            item.add_marker(skip_mysql)
        if "redis_required" in item.keywords and not redis_ok:
            item.add_marker(skip_redis)
