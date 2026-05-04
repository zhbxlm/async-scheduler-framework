"""Main API server entry point."""
import uvicorn
from fastapi import FastAPI
from config.settings import settings

app = FastAPI(title="Ray AMU API")


@app.get("/health")
async def health():
    return {"status": "ok"}


if __name__ == "__main__":
    uvicorn.run(
        "src.main:app",
        host=settings.infra.server.api_host,
        port=settings.infra.server.api_port,
        reload=False,
    )
