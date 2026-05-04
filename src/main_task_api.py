# rebuilt from deepwiki-reference alignment
"""Task API server — task submission endpoint (port 8001).

Separate from the ops API (src/main.py), this service focuses exclusively
on the /api/v1/tasks path: task creation, query, result retrieval and cancel.
"""
from __future__ import annotations
from contextlib import asynccontextmanager
from fastapi import FastAPI


@asynccontextmanager
async def lifespan(app: FastAPI):
    from src.common.async_db import init_async_engine
    from config.settings_compat import settings
    if settings.mysql.url:
        init_async_engine(settings.mysql.url)
    yield


app = FastAPI(
    title="Ray Async Task API",
    version="0.1.0",
    description="Task submission and query API for the Ray Async scheduling framework.",
    lifespan=lifespan,
)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "task-api"}


# ── Task routes (/api/v1/tasks) are mounted from src.api.routes.tasks ──────
try:
    from src.api.routes.tasks import router as tasks_router
    app.include_router(tasks_router, prefix="/api/v1")
except Exception:
    # Graceful degradation when DB/dependencies not configured
    pass


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("src.main_task_api:app", host="0.0.0.0", port=8001, reload=False)
