"""Shared fake Redis client for unit tests.

Provides a complete in-process implementation of the Redis API subset used
by RedisQueueBackend, so that _client_supports_queue_ops evaluates to True
and all queue logic (including Lua-path detection via hasattr(client, 'eval'))
is exercised without a real Redis instance.
"""
from __future__ import annotations


class FullFakeAsyncRedis:
    """Complete fake covering every method checked by _client_supports_queue_ops."""

    def __init__(self) -> None:
        self.lists: dict[str, list[str]] = {}
        self.zsets: dict[str, dict[str, float]] = {}
        self.sets: dict[str, set[str]] = {}
        self.hashes: dict[str, dict[str, str]] = {}
        self._strings: dict[str, str] = {}
        self._expiry: dict[str, int] = {}

    # --- List ops ---

    async def rpush(self, key: str, value: str) -> int:
        self.lists.setdefault(key, []).append(value)
        return len(self.lists[key])

    async def lpop(self, key: str):
        values = self.lists.get(key, [])
        if not values:
            return None
        return values.pop(0)

    async def llen(self, key: str) -> int:
        return len(self.lists.get(key, []))

    async def lrem(self, key: str, count: int, value: str) -> int:
        values = self.lists.get(key, [])
        removed = 0
        kept: list[str] = []
        for item in values:
            if item == value and (count == 0 or removed < count):
                removed += 1
                continue
            kept.append(item)
        self.lists[key] = kept
        return removed

    async def lrange(self, key: str, start: int, stop: int) -> list[str]:
        lst = self.lists.get(key, [])
        if stop == -1:
            return lst[start:]
        return lst[start: stop + 1]

    # --- Sorted set ops ---

    async def zadd(self, key: str, mapping: dict[str, float]) -> int:
        bucket = self.zsets.setdefault(key, {})
        for member, score in mapping.items():
            bucket[member] = score
        return len(mapping)

    async def zrangebyscore(
        self,
        key: str,
        min_score,
        max_score,
        start: int = 0,
        num: int = -1,
        withscores: bool = False,
    ):
        bucket = self.zsets.get(key, {})
        min_v = float("-inf") if min_score in ("-inf", b"-inf") else float(min_score)
        max_v = float("+inf") if max_score in ("+inf", b"+inf") else float(max_score)
        items = sorted(
            [(m, s) for m, s in bucket.items() if min_v <= s <= max_v],
            key=lambda x: x[1],
        )
        # pagination
        if start:
            items = items[start:]
        if num >= 0:
            items = items[:num]
        if withscores:
            return items  # list of (member, score)
        return [m for m, _ in items]

    async def zrem(self, key: str, member: str) -> int:
        bucket = self.zsets.get(key, {})
        existed = member in bucket
        bucket.pop(member, None)
        return 1 if existed else 0

    async def zcard(self, key: str) -> int:
        return len(self.zsets.get(key, {}))

    async def zpopmin(self, key: str, count: int = 1):
        bucket = self.zsets.get(key, {})
        if not bucket:
            return []
        sorted_items = sorted(bucket.items(), key=lambda x: x[1])
        result = []
        for member, score in sorted_items[:count]:
            result.append((member, score))
            del bucket[member]
        return result

    # --- Set ops ---

    async def sadd(self, key: str, member: str) -> int:
        self.sets.setdefault(key, set()).add(member)
        return 1

    async def srem(self, key: str, member: str) -> int:
        if key not in self.sets or member not in self.sets[key]:
            return 0
        self.sets[key].remove(member)
        return 1

    async def sismember(self, key: str, member: str) -> bool:
        return member in self.sets.get(key, set())

    async def scard(self, key: str) -> int:
        return len(self.sets.get(key, set()))

    async def smembers(self, key: str) -> set[str]:
        return set(self.sets.get(key, set()))

    # --- Hash ops ---

    async def hset(self, key: str, field: str, value: str) -> int:
        self.hashes.setdefault(key, {})[field] = value
        return 1

    async def hget(self, key: str, field: str) -> str | None:
        return self.hashes.get(key, {}).get(field)

    async def hdel(self, key: str, field: str) -> int:
        if key not in self.hashes or field not in self.hashes[key]:
            return 0
        del self.hashes[key][field]
        return 1

    async def hexists(self, key: str, field: str) -> bool:
        return field in self.hashes.get(key, {})

    async def hgetall(self, key: str) -> dict[str, str]:
        return dict(self.hashes.get(key, {}))

    async def hincrby(self, key: str, field: str, amount: int = 1) -> int:
        bucket = self.hashes.setdefault(key, {})
        current = int(bucket.get(field, 0))
        new_val = current + amount
        bucket[field] = str(new_val)
        return new_val

    # --- String ops ---

    async def get(self, key: str) -> str | None:
        return self._strings.get(key)

    async def set(self, key: str, value: str, ex: int | None = None, nx: bool = False, xx: bool = False) -> bool:
        if nx and key in self._strings:
            return False
        if xx and key not in self._strings:
            return False
        self._strings[key] = value
        if ex is not None:
            self._expiry[key] = ex
        return True

    async def expire(self, key: str, seconds: int) -> int:
        if key in self._strings or key in self.hashes or key in self.sets or key in self.lists:
            self._expiry[key] = seconds
            return 1
        return 0

    async def pexpire(self, key: str, milliseconds: int) -> int:
        """Set TTL in milliseconds (stored as seconds for in-memory simplicity)."""
        seconds = max(1, milliseconds // 1000)
        return await self.expire(key, seconds)

    async def pttl(self, key: str) -> int:
        if key not in self._strings and key not in self.hashes:
            return -2
        if key not in self._expiry:
            return -1
        return self._expiry[key] * 1000

    # --- Generic ops ---

    async def exists(self, key: str) -> int:
        return int(
            key in self.lists
            or key in self.zsets
            or key in self.sets
            or key in self.hashes
            or key in self._strings
        )

    async def delete(self, *keys: str) -> int:
        count = 0
        for key in keys:
            for store in (self.lists, self.zsets, self.sets, self.hashes, self._strings):
                if key in store:
                    del store[key]  # type: ignore[arg-type]
                    count += 1
        return count

    async def flushdb(self) -> None:
        self.lists.clear()
        self.zsets.clear()
        self.sets.clear()
        self.hashes.clear()
        self._strings.clear()
        self._expiry.clear()

    async def scan(self, cursor: int, match: str = "*", count: int = 100):
        """Return (next_cursor, [matching_keys]).

        Simplification: always returns all matching keys in one call (cursor always 0 out).
        Supports '*' glob patterns by converting to prefix/suffix matching.
        """
        import fnmatch
        all_keys: list[str] = []
        for store in (self.lists, self.zsets, self.sets, self.hashes, self._strings):
            all_keys.extend(store.keys())
        # Deduplicate
        seen: set[str] = set()
        unique: list[str] = []
        for k in all_keys:
            if k not in seen:
                seen.add(k)
                unique.append(k)
        matched = [k for k in unique if fnmatch.fnmatch(k, match)]
        return 0, matched  # cursor=0 means full cycle done

    async def incrby(self, key: str, amount: int = 1) -> int:
        current = int(self._strings.get(key, "0"))
        new_val = current + amount
        self._strings[key] = str(new_val)
        return new_val

    async def incr(self, key: str) -> int:
        return await self.incrby(key, 1)

    async def decr(self, key: str) -> int:
        return await self.incrby(key, -1)

    def pipeline(self) -> "FakePipeline":
        return FakePipeline(self)

    def register_script(self, script: str):
        """Return a callable that simulates Redis Lua script execution.

        Supports three Lua patterns used in this codebase:

        1. Acquire-slot  (script contains 'INCR'): increment counter if below limit
        2. Release-slot  (script contains 'DECR'): decrement counter
        3. Leader-renew  (script contains 'EXPIRE' and 'GET' but not 'INCR'/'DECR'):
           GET key → compare with ARGV[0] → if match, EXPIRE and return 1, else 0
        """
        redis_ref = self
        is_acquire = "INCR" in script
        is_release = "DECR" in script and "INCR" not in script
        is_leader_renew = "EXPIRE" in script and "GET" in script and not is_acquire and not is_release
        _CONC_KEY = "global:consumer:concurrency"

        async def _script(keys=None, args=None):
            keys = keys or []
            args = args or []
            key = keys[0] if keys else _CONC_KEY

            if is_leader_renew:
                # Pattern: GET key; if value == ARGV[0], EXPIRE key ARGV[1], return 1 else 0
                current = redis_ref._strings.get(key)
                expected = args[0] if args else None
                if current is not None and current == expected:
                    ttl = int(args[1]) if len(args) > 1 else 60
                    redis_ref._expiry[key] = ttl
                    return 1
                return 0

            current = int(redis_ref._strings.get(key, "0"))
            if is_acquire:
                limit = int(args[0]) if args else 1
                if current < limit:
                    redis_ref._strings[key] = str(current + 1)
                    return 1
                return 0
            else:
                # Release: decrement if > 0
                if current > 0:
                    redis_ref._strings[key] = str(current - 1)
                    return current - 1
                return 0

        return _script


class FakePipeline:
    """Minimal pipeline that collects commands and executes them sequentially."""

    def __init__(self, client: FullFakeAsyncRedis) -> None:
        self._client = client
        self._commands: list[tuple] = []

    def llen(self, key: str) -> "FakePipeline":
        self._commands.append(("llen", key))
        return self

    def zcard(self, key: str) -> "FakePipeline":
        self._commands.append(("zcard", key))
        return self

    def get(self, key: str) -> "FakePipeline":
        self._commands.append(("get", key))
        return self

    def set(self, key: str, value: str, **kwargs) -> "FakePipeline":
        self._commands.append(("set", key, value, kwargs))
        return self

    def exists(self, key: str) -> "FakePipeline":
        self._commands.append(("exists", key))
        return self

    def zadd(self, key: str, mapping: dict) -> "FakePipeline":
        self._commands.append(("zadd", key, mapping))
        return self

    async def execute(self) -> list:
        results = []
        for cmd, *args in self._commands:
            if cmd == "set":
                key, value, kwargs = args
                method = getattr(self._client, "set")
                results.append(await method(key, value, **kwargs))
            elif cmd == "zadd":
                key, mapping = args
                results.append(await self._client.zadd(key, mapping))
            else:
                method = getattr(self._client, cmd)
                results.append(await method(*args))
        return results
