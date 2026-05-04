# ── Stage 1: builder ──────────────────────────────────────────────────────
FROM python:3.11-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /build

# Install build deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    default-libmysqlclient-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Copy package manifests first (better layer caching)
COPY pyproject.toml setup.py README.md ./

# Copy source packages
COPY config/ ./config/
COPY src/     ./src/

# Install into a prefix for clean copy
RUN pip install --prefix=/install --no-warn-script-location .

# ── Stage 2: api runtime ──────────────────────────────────────────────────
FROM python:3.11-slim AS api

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    default-libmysqlclient-dev \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /install /usr/local
COPY config/ ./config/
COPY src/     ./src/

EXPOSE 8000
HEALTHCHECK --interval=15s --timeout=5s --retries=3 \
    CMD curl -fs http://localhost:8000/health || exit 1

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]

# ── Stage 3: task-api runtime ─────────────────────────────────────────────
FROM api AS task-api

EXPOSE 8001
CMD ["uvicorn", "src.main_task_api:app", "--host", "0.0.0.0", "--port", "8001"]

# ── Stage 4: agent runtime ────────────────────────────────────────────────
FROM api AS agent

RUN apt-get update && apt-get install -y --no-install-recommends \
    ray \
    && rm -rf /var/lib/apt/lists/* || true

EXPOSE 9100
HEALTHCHECK --interval=15s --timeout=5s --retries=3 \
    CMD curl -fs http://localhost:9100/health || exit 1

CMD ["uvicorn", "src.agent.server:app", "--host", "0.0.0.0", "--port", "9100"]
