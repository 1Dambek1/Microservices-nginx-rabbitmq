from __future__ import annotations

import asyncio
import json
import os
import sqlite3
from contextlib import asynccontextmanager, suppress
from pathlib import Path
from typing import Any

import aio_pika
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = Path(os.getenv("USERS_DB_PATH", str(BASE_DIR / "users.db")))
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
USERS_RPC_QUEUE = os.getenv("USERS_RPC_QUEUE", "users.rpc")
ROOT_PATH = os.getenv("ROOT_PATH", "/users")
SEED_DEMO_DATA = os.getenv("SEED_DEMO_DATA", "false").lower() == "true"


class UserCreate(BaseModel):
    name: str
    email: str


def get_connection() -> sqlite3.Connection:
    connection = sqlite3.connect(DB_PATH, check_same_thread=False)
    connection.row_factory = sqlite3.Row
    return connection


def init_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with get_connection() as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT NOT NULL UNIQUE
            )
            """
        )
        connection.commit()


def serialize_user(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": row["id"],
        "name": row["name"],
        "email": row["email"],
    }


def list_users() -> list[dict[str, Any]]:
    with get_connection() as connection:
        rows = connection.execute(
            "SELECT id, name, email FROM users ORDER BY id"
        ).fetchall()
    return [serialize_user(row) for row in rows]


def get_user_by_id(user_id: int) -> dict[str, Any] | None:
    with get_connection() as connection:
        row = connection.execute(
            "SELECT id, name, email FROM users WHERE id = ?",
            (user_id,),
        ).fetchone()
    if row is None:
        return None
    return serialize_user(row)


def create_user_in_db(name: str, email: str) -> dict[str, Any]:
    with get_connection() as connection:
        cursor = connection.execute(
            "INSERT INTO users (name, email) VALUES (?, ?)",
            (name, email),
        )
        connection.commit()
        user_id = int(cursor.lastrowid)
    user = get_user_by_id(user_id)
    if user is None:
        raise RuntimeError("User was created but could not be loaded")
    return user


def ensure_demo_users() -> int:
    demo_users = [
        ("Alice Johnson", "alice@example.com"),
        ("Bob Smith", "bob@example.com"),
        ("Charlie Brown", "charlie@example.com"),
    ]
    with get_connection() as connection:
        existing_count = connection.execute(
            "SELECT COUNT(*) FROM users"
        ).fetchone()[0]
        if existing_count == 0:
            connection.executemany(
                "INSERT INTO users (name, email) VALUES (?, ?)",
                demo_users,
            )
            connection.commit()
        total = connection.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    return int(total)


async def handle_rpc_message(
    message: aio_pika.IncomingMessage,
    exchange: aio_pika.abc.AbstractExchange,
) -> None:
    async with message.process(requeue=False):
        response: dict[str, Any]
        try:
            payload = json.loads(message.body.decode("utf-8"))
        except json.JSONDecodeError:
            response = {"ok": False, "error": "invalid_json"}
        else:
            action = payload.get("action")
            if action == "get_user":
                user_id = int(payload.get("user_id", 0))
                user = get_user_by_id(user_id)
                if user is None:
                    response = {"ok": False, "error": "user_not_found"}
                else:
                    response = {"ok": True, "user": user}
            else:
                response = {"ok": False, "error": "unknown_action"}

        if message.reply_to:
            reply = aio_pika.Message(
                body=json.dumps(response).encode("utf-8"),
                correlation_id=message.correlation_id,
                content_type="application/json",
            )
            await exchange.publish(
                reply,
                routing_key=message.reply_to,
            )


async def consume_user_rpc(app: FastAPI) -> None:
    connection: aio_pika.RobustConnection | None = None
    try:
        while True:
            try:
                connection = await aio_pika.connect_robust(RABBITMQ_URL)
                app.state.rabbitmq_connection = connection

                channel = await connection.channel()
                await channel.set_qos(prefetch_count=10)
                queue = await channel.declare_queue(USERS_RPC_QUEUE, durable=True)
                await queue.consume(
                    lambda message: handle_rpc_message(message, channel.default_exchange)
                )

                await asyncio.Future()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                print(f"users_service RPC consumer error: {exc}")
                await asyncio.sleep(2)
            finally:
                if connection is not None and not connection.is_closed:
                    await connection.close()
                    connection = None
    finally:
        app.state.rabbitmq_connection = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    if SEED_DEMO_DATA:
        ensure_demo_users()

    rpc_task = asyncio.create_task(consume_user_rpc(app))
    app.state.rpc_task = rpc_task
    app.state.rabbitmq_connection = None

    yield

    rpc_task.cancel()
    with suppress(asyncio.CancelledError):
        await rpc_task


app = FastAPI(title="Users Service", version="1.0.0", lifespan=lifespan, root_path=ROOT_PATH)


@app.get("/")
def root() -> dict[str, Any]:
    return {
        "service": "users_service",
        "message": "Users microservice is running",
    }


@app.get("/health")
def health() -> dict[str, Any]:
    connection = getattr(app.state, "rabbitmq_connection", None)
    rabbitmq_connected = bool(connection is not None and not connection.is_closed)
    return {
        "status": "ok",
        "service": "users_service",
        "rabbitmq_connected": rabbitmq_connected,
        "users_rpc_queue": USERS_RPC_QUEUE,
    }


@app.get("/users")
def get_users() -> list[dict[str, Any]]:
    return list_users()


@app.get("/users/{user_id}")
def get_user(user_id: int) -> dict[str, Any]:
    user = get_user_by_id(user_id)
    if user is None:
        raise HTTPException(status_code=404, detail="User not found")
    return user


@app.post("/users")
def create_user(payload: UserCreate) -> dict[str, Any]:
    try:
        return create_user_in_db(payload.name, payload.email)
    except sqlite3.IntegrityError as exc:
        raise HTTPException(status_code=400, detail="Email already exists") from exc


@app.post("/seed-demo")
def seed_demo() -> dict[str, Any]:
    total = ensure_demo_users()
    return {
        "message": "Demo users are ready",
        "total_users": total,
        "users": list_users(),
    }
