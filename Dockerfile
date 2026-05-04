# syntax=docker/dockerfile:1
# =====================================================================
# Multi-stage Dockerfile for async-scheduler-framework
# Stages:
#   builder  — install Python deps with pip
#   runtime  — minimal runtime image
#   dev      — development image with hot-reload
# =====================================================================

# ---- base -------------------------------------------------------
FROM python:3.11-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# ---- builder ----------------------------------------------------
FROM base AS builder

# Install build tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python deps
COPY requirements.txt requirements-prod.txt* ./
RUN pip install --prefix=/install -r requirements.txt \
    && if [ -f requirements-prod.txt ]; then pip install --prefix=/install -r requirements-prod.txt; fi

# ---- runtime ----------------------------------------------------
FROM base AS runtime

# Copy installed packages from builder
COPY --from=builder /install /usr/local

# Copy application source
COPY src/ ./src/
COPY config/ ./config/
COPY alembic.ini* ./
COPY alembic/ ./alembic/

# Non-root user for security
RUN addgroup --system --gid 1001 appgroup \
    && adduser --system --uid 1001 --ingroup appgroup --no-create-home appuser

USER appuser

EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health/ready')" \
    || exit 1

# Default: main API
CMD ["python", "-m", "uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]

# ---- dev --------------------------------------------------------
FROM runtime AS dev

USER root

RUN pip install watchfiles pytest pytest-asyncio httpx

# Mount source for hot-reload
VOLUME ["/app/src", "/app/config", "/app/tests"]

USER appuser

CMD ["python", "-m", "uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
