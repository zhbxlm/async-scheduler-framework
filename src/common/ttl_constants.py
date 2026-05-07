"""Centralized TTL constants (in seconds).

Keeping all Redis TTL values in one place avoids magic numbers scattered
across services and makes it easy to tune them via a single PR.
"""

# Task / queue layer
TASK_REDIS_TTL = 86400          # 24 h  — active task record in Redis
CALLBACK_DONE_TTL = 86400       # 24 h  — callback-done dedup marker

# Transaction / compensation layer
TX_PENDING_TTL = 300            # 5 min — pending distributed-write tx record
CALLBACK_RETRY_PAYLOAD_TTL = 3600  # 1 h  — callback retry payload companion key

# Alert layer
ALERT_ACK_TTL = 86400           # 24 h  — alert acknowledgement marker
