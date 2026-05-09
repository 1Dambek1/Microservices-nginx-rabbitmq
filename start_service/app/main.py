from typing import Any

from fastapi import FastAPI




app = FastAPI(title="Service", version="1.0.0")


@app.get("/")
def root() -> dict[str, Any]:
    return {
        "service": "start",
        "message": "start microservice is running",
    }


@app.get("/health")
def health() -> dict[str, Any]:
    return {
        "status": "ok",

    }

