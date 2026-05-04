"""Task‑only API server (separate deployment)."""
import uvicorn
from fastapi import FastAPI

app = FastAPI(title="Ray AMU Task API")


@app.get("/health")
async def health():
    return {"status": "task_api_ok"}


if __name__ == "__main__":
    uvicorn.run(
        "src.main_task_api:app",
        host="0.0.0.0",
        port=8001,
        reload=False,
    )
