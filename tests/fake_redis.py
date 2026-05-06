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

    async def set(self, key: str, value: str, ex: int | None = None, px: int | None = None,
                   nx: bool = False, xx: bool = False) -> bool:
        if nx and key in self._strings:
            return False
        if xx and key not in self._strings:
            return False
        self._strings[key] = value
        if ex is not None:
            self._expiry[key] = ex
        elif px is not None:
            self._expiry[key] = max(1, px // 1000)
        return True

    async def expire(self, key: str, seconds: int) -> int:
        if key in self._strings or key in self.hashes or key in self.sets or key in self.lists:
            self._expiry[key] = seconds
            return 1
        return 0

    async def setex(self, key: str, seconds: int, value: str) -> bool:
        self._strings[key] = value
        self._expiry[key] = seconds
        return True

    async def scan_iter(self, pattern: str = "*"):
        """Async generator yielding matching keys (simplified glob match)."""
        import fnmatch
        all_keys: list[str] = []
        for store in (self.lists, self.zsets, self.sets, self.hashes, self._strings):
            all_keys.extend(store.keys())
        seen: set[str] = set()
        for k in all_keys:
            if k not in seen and fnmatch.fnmatch(k, pattern):
                seen.add(k)
                yield k

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
        """Return a callable simulating Redis Lua script execution.

        Pattern detection (by script content keywords):
        1. INCR  → acquire-slot: increment counter if below limit
        2. DECR  → release-slot: decrement counter
        3. EXPIRE+GET (no INCR/DECR/DEL) → leader-renew: compare+expire
        4. DEL   → lock-release (CAS): GET → compare → DEL if match
        5. PEXPIRE → lock-renew: GET → compare → PEXPIRE if match, return 'ok'/'lost'
        6. cjson → JSON Lua: stub returns 1 (toggle/advance scripts)
        7. HGET/HSET/ZADD/ZCARD/ZRANGEBYSCORE → queue Lua: Python implementation
        """
        redis_ref = self
        is_acquire  = "INCR" in script and "DECR" not in script
        is_release_slot = "DECR" in script and "INCR" not in script
        is_leader_renew = "EXPIRE" in script and "GET" in script and "DEL" not in script and "PEXPIRE" not in script and not is_acquire and not is_release_slot
        is_lock_release = "DEL" in script and "GET" in script and "PEXPIRE" not in script and not is_acquire
        is_lock_renew   = "PEXPIRE" in script and "GET" in script
        is_json_lua     = "cjson" in script
        is_enqueue      = "ZADD" in script and "ZCARD" in script and "EXPIRE" in script
        is_dequeue      = "ZRANGEBYSCORE" in script and "ZADD" in script and "ZREM" in script
        is_complete     = "ZREM" in script and "HINCRBY" in script
        is_cancel       = "ZREM" in script and "HINCRBY" not in script and "ZADD" not in script and "HSET" not in script
        is_adjust       = "HSET" in script and "HGET" in script and "max_concurrent" in script and "ZCARD" not in script and "ZADD" not in script
        is_acquire_slot = "ZADD" in script and "HGET" in script and "max_concurrent" in script
        is_recover      = "max_concurrent_baseline" in script
        _CONC_KEY = "global:consumer:concurrency"

        async def _script(keys=None, args=None):
            keys = keys or []
            args = args or []

            # --- leader-renew (CronScheduler / TaskReconciler) ---
            if is_leader_renew:
                key = keys[0] if keys else ""
                current = redis_ref._strings.get(key)
                expected = args[0] if args else None
                if current is not None and current == expected:
                    ttl = int(args[1]) if len(args) > 1 else 60
                    redis_ref._expiry[key] = ttl
                    return 1
                return 0

            # --- lock-release CAS (TaskExecutor) ---
            if is_lock_release:
                key = keys[0] if keys else ""
                current = redis_ref._strings.get(key)
                expected = args[0] if args else None
                if current == expected:
                    redis_ref._strings.pop(key, None)
                    return 1
                return 0

            # --- lock-renew (TaskExecutor _renew_lock_loop) ---
            if is_lock_renew:
                key = keys[0] if keys else ""
                current = redis_ref._strings.get(key)
                expected = args[0] if args else None
                if current == expected:
                    ttl_ms = int(args[1]) if len(args) > 1 else 30000
                    redis_ref._expiry[key] = max(1, ttl_ms // 1000)
                    return b"ok"
                return b"lost"

            # --- JSON Lua stubs (ScheduleRegistry toggle/advance) ---
            if is_json_lua:
                return 1  # simulate success

            # --- QueueManager: enqueue ---
            if is_enqueue:
                pending_key, stats_key, config_key, running_key = keys[0], keys[1], keys[2], keys[3]
                task_id = args[0]
                score = float(args[1])
                default_depth = int(args[2])
                ttl = int(args[3])
                bucket = redis_ref.zsets.setdefault(pending_key, {})
                cfg = redis_ref.hashes.get(config_key, {})
                max_depth = int(cfg.get("max_queue_depth", default_depth))
                cur = len(bucket)
                if cur >= max_depth:
                    return [-1, cur]
                bucket[task_id] = score
                h = redis_ref.hashes.setdefault(stats_key, {})
                h["total_enqueued"] = str(int(h.get("total_enqueued", "0")) + 1)
                for k in (pending_key, stats_key, config_key):
                    redis_ref._expiry[k] = ttl
                cnt = len(bucket)
                return [cur + 1, cnt]

            # --- QueueManager: dequeue_ready ---
            if is_dequeue:
                pending_key, running_key, stats_key, config_key = keys[0], keys[1], keys[2], keys[3]
                default_mc = int(args[0])
                now_ms = int(args[1])
                scan_limit = int(args[2])
                cfg = redis_ref.hashes.get(config_key, {})
                max_concurrent = int(cfg.get("max_concurrent", default_mc))
                running_bucket = redis_ref.zsets.get(running_key, {})
                if len(running_bucket) >= max_concurrent:
                    return None
                pending_bucket = redis_ref.zsets.get(pending_key, {})
                sorted_items = sorted(pending_bucket.items(), key=lambda x: x[1])
                for tid, sc in sorted_items[:scan_limit]:
                    ts = int(sc) % 10_000_000_000_000
                    if ts <= now_ms:
                        del pending_bucket[tid]
                        redis_ref.zsets.setdefault(running_key, {})[tid] = float(now_ms)
                        h = redis_ref.hashes.setdefault(stats_key, {})
                        h["total_dequeued"] = str(int(h.get("total_dequeued", "0")) + 1)
                        return tid.encode() if isinstance(tid, str) else tid
                return None

            # --- QueueManager: complete/fail ---
            if is_complete:
                running_key, stats_key = keys[0], keys[1]
                task_id = args[0]
                counter = args[1] if len(args) > 1 else "total_completed"
                rb = redis_ref.zsets.get(running_key, {})
                rb.pop(task_id, None)
                h = redis_ref.hashes.setdefault(stats_key, {})
                h[counter] = str(int(h.get(counter, "0")) + 1)
                return 1

            # --- QueueManager: cancel ---
            if is_cancel:
                pending_key, running_key = keys[0], keys[1]
                task_id = args[0]
                pb = redis_ref.zsets.get(pending_key, {})
                if task_id in pb:
                    del pb[task_id]
                    return 1
                rb = redis_ref.zsets.get(running_key, {})
                if task_id in rb:
                    del rb[task_id]
                    return 1
                return 0

            # --- QueueManager: adjust_concurrent ---
            if is_adjust:
                config_key = keys[0]
                delta = int(args[0])
                default_val = int(args[1]) if len(args) > 1 else 8
                h = redis_ref.hashes.setdefault(config_key, {})
                cur = int(h.get("max_concurrent", default_val))
                new_val = max(1, cur + delta)
                h["max_concurrent"] = str(new_val)
                return new_val

            # --- QueueManager: acquire_slot ---
            if is_acquire_slot:
                running_key, config_key = keys[0], keys[1]
                slot_id = args[0]
                default_mc = int(args[1])
                now_ts = float(args[2])
                ttl = int(args[3]) if len(args) > 3 else 3600
                cfg = redis_ref.hashes.get(config_key, {})
                max_concurrent = int(cfg.get("max_concurrent", default_mc))
                rb = redis_ref.zsets.setdefault(running_key, {})
                if len(rb) >= max_concurrent:
                    return -1
                rb[slot_id] = now_ts
                for k in (running_key, config_key):
                    redis_ref._expiry[k] = ttl
                return 1

            # --- QueueManager: recover_concurrent ---
            if is_recover:
                config_key = keys[0]
                default_baseline = int(args[0]) if args else 8
                h = redis_ref.hashes.setdefault(config_key, {})
                baseline = int(h.get("max_concurrent_baseline", default_baseline))
                cur = int(h.get("max_concurrent", baseline))
                if cur < baseline:
                    new_val = cur + 1
                    h["max_concurrent"] = str(new_val)
                    return new_val
                return cur

            # --- slot acquire/release (TaskConsumer global concurrency) ---
            key = keys[0] if keys else _CONC_KEY
            current = int(redis_ref._strings.get(key, "0"))
            if is_acquire:
                limit = int(args[0]) if args else 1
                if current < limit:
                    redis_ref._strings[key] = str(current + 1)
                    return 1
                return 0
            else:
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

    def hset(self, key: str, field: str, value: str) -> "FakePipeline":
        self._commands.append(("hset", key, field, value))
        return self

    def hdel(self, key: str, field: str) -> "FakePipeline":
        self._commands.append(("hdel", key, field))
        return self

    def sadd(self, key: str, member: str) -> "FakePipeline":
        self._commands.append(("sadd", key, member))
        return self

    def srem(self, key: str, member: str) -> "FakePipeline":
        self._commands.append(("srem", key, member))
        return self

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
