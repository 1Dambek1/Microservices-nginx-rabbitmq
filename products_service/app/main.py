from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import aio_pika
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

APP_INSTANCE = os.getenv("APP_INSTANCE", "products-service")
BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = Path(
    os.getenv("PRODUCTS_DB_PATH", str(BASE_DIR / f"{APP_INSTANCE.replace('-', '_')}.db"))
)
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
USERS_RPC_QUEUE = os.getenv("USERS_RPC_QUEUE", "users.rpc")


class ProductCreate(BaseModel):
    name: str
    price: float
    owner_user_id: int


def get_connection() -> sqlite3.Connection:
    connection = sqlite3.connect(DB_PATH, check_same_thread=False)
    connection.row_factory = sqlite3.Row
    return connection


def init_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with get_connection() as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                price REAL NOT NULL,
                owner_user_id INTEGER NOT NULL
            )
            """
        )
        connection.commit()


def serialize_product(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": row["id"],
        "name": row["name"],
        "price": row["price"],
        "owner_user_id": row["owner_user_id"],
        "served_by": APP_INSTANCE,
    }


def list_products() -> list[dict[str, Any]]:
    with get_connection() as connection:
        rows = connection.execute(
            "SELECT id, name, price, owner_user_id FROM products ORDER BY id"
        ).fetchall()
    return [serialize_product(row) for row in rows]


def get_product_by_id(product_id: int) -> dict[str, Any] | None:
    with get_connection() as connection:
        row = connection.execute(
            "SELECT id, name, price, owner_user_id FROM products WHERE id = ?",
            (product_id,),
        ).fetchone()
    if row is None:
        return None
    return serialize_product(row)


def create_product_in_db(name: str, price: float, owner_user_id: int) -> dict[str, Any]:
    with get_connection() as connection:
        cursor = connection.execute(
            "INSERT INTO products (name, price, owner_user_id) VALUES (?, ?, ?)",
            (name, price, owner_user_id),
        )
        connection.commit()
        product_id = int(cursor.lastrowid)
    product = get_product_by_id(product_id)
    if product is None:
        raise RuntimeError("Product was created but could not be loaded")
    return product


def ensure_demo_products() -> int:
    demo_products = [
        ("Laptop Stand", 49.90, 1),
        ("Gaming Mouse", 79.50, 2),
        ("USB-C Hub", 39.99, 1),
    ]
    with get_connection() as connection:
        existing_count = connection.execute(
            "SELECT COUNT(*) FROM products"
        ).fetchone()[0]
        if existing_count == 0:
            connection.executemany(
                "INSERT INTO products (name, price, owner_user_id) VALUES (?, ?, ?)",
                demo_products,
            )
            connection.commit()
        total = connection.execute("SELECT COUNT(*) FROM products").fetchone()[0]
    return int(total)


class UserRpcClient:
    def __init__(self, rabbitmq_url: str, request_queue: str) -> None:
        self.rabbitmq_url = rabbitmq_url
        self.request_queue = request_queue
        self.connection: aio_pika.RobustConnection | None = None
        self.channel: aio_pika.abc.AbstractRobustChannel | None = None
        self.callback_queue: aio_pika.abc.AbstractQueue | None = None
        self.futures: dict[str, asyncio.Future[dict[str, Any]]] = {}

    @property
    def is_connected(self) -> bool:
        return bool(
            self.connection is not None
            and not self.connection.is_closed
            and self.channel is not None
            and not self.channel.is_closed
            and self.callback_queue is not None
        )

    async def connect(self) -> None:
        if self.is_connected:
            return

        self.connection = await aio_pika.connect_robust(self.rabbitmq_url)
        self.channel = await self.connection.channel()
        self.callback_queue = await self.channel.declare_queue(exclusive=True)
        await self.callback_queue.consume(self.on_response, no_ack=True)

    async def connect_with_retry(self) -> None:
        while True:
            try:
                await self.connect()
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                print(f"{APP_INSTANCE} RPC client connection error: {exc}")
                await asyncio.sleep(2)

    async def on_response(self, message: aio_pika.IncomingMessage) -> None:
        correlation_id = message.correlation_id
        if not correlation_id:
            return

        future = self.futures.pop(correlation_id, None)
        if future is None or future.done():
            return

        try:
            payload = json.loads(message.body.decode("utf-8"))
        except json.JSONDecodeError:
            future.set_result({"ok": False, "error": "invalid_json"})
        else:
            future.set_result(payload)

    async def request(self, payload: dict[str, Any], timeout: float = 5.0) -> dict[str, Any]:
        if not self.is_connected:
            await self.connect_with_retry()

        if self.channel is None or self.callback_queue is None:
            raise RuntimeError("RabbitMQ callback queue is not ready")

        correlation_id = str(uuid.uuid4())
        loop = asyncio.get_running_loop()
        future: asyncio.Future[dict[str, Any]] = loop.create_future()
        self.futures[correlation_id] = future

        message = aio_pika.Message(
            body=json.dumps(payload).encode("utf-8"),
            content_type="application/json",
            correlation_id=correlation_id,
            reply_to=self.callback_queue.name,
        )

        await self.channel.default_exchange.publish(
            message,
            routing_key=self.request_queue,
        )

        try:
            return await asyncio.wait_for(future, timeout=timeout)
        except asyncio.TimeoutError as exc:
            raise RuntimeError("users_service RPC timeout") from exc
        finally:
            self.futures.pop(correlation_id, None)

    async def close(self) -> None:
        if self.connection is not None and not self.connection.is_closed:
            await self.connection.close()
        self.connection = None
        self.channel = None
        self.callback_queue = None


async def fetch_owner_or_raise(rpc_client: UserRpcClient, owner_user_id: int) -> dict[str, Any]:
    try:
        response = await rpc_client.request(
            {"action": "get_user", "user_id": owner_user_id}
        )
    except RuntimeError as exc:
        raise HTTPException(
            status_code=503,
            detail=f"users_service is unavailable: {exc}",
        ) from exc

    if not response.get("ok"):
        error = response.get("error")
        if error == "user_not_found":
            raise HTTPException(status_code=404, detail="Owner user not found")
        raise HTTPException(status_code=502, detail=f"users_service RPC error: {error}")

    return response["user"]


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    ensure_demo_products()

    user_rpc = UserRpcClient(RABBITMQ_URL, USERS_RPC_QUEUE)
    await user_rpc.connect_with_retry()
    app.state.user_rpc = user_rpc

    yield

    await user_rpc.close()


app = FastAPI(title="Products Service", version="1.0.0", lifespan=lifespan)


@app.get("/")
def root() -> dict[str, Any]:
    return {
        "service": "products_service",
        "instance": APP_INSTANCE,
        "message": "Products microservice is running behind nginx",
    }


@app.get("/health")
def health() -> dict[str, Any]:
    user_rpc: UserRpcClient = app.state.user_rpc
    return {
        "status": "ok",
        "service": "products_service",
        "instance": APP_INSTANCE,
        "rabbitmq_connected": user_rpc.is_connected,
        "users_rpc_queue": USERS_RPC_QUEUE,
    }


@app.get("/products")
def get_products() -> list[dict[str, Any]]:
    return list_products()


@app.get("/products/{product_id}")
async def get_product(product_id: int) -> dict[str, Any]:
    product = get_product_by_id(product_id)
    if product is None:
        raise HTTPException(status_code=404, detail="Product not found")

    user_rpc: UserRpcClient = app.state.user_rpc
    owner = await fetch_owner_or_raise(user_rpc, product["owner_user_id"])
    return {
        **product,
        "owner": owner,
    }


@app.post("/products")
async def create_product(payload: ProductCreate) -> dict[str, Any]:
    user_rpc: UserRpcClient = app.state.user_rpc
    owner = await fetch_owner_or_raise(user_rpc, payload.owner_user_id)
    product = create_product_in_db(payload.name, payload.price, payload.owner_user_id)
    return {
        **product,
        "owner": owner,
    }


@app.post("/seed-demo")
def seed_demo() -> dict[str, Any]:
    total = ensure_demo_products()
    return {
        "message": "Demo products are ready on this instance",
        "instance": APP_INSTANCE,
        "total_products": total,
        "products": list_products(),
    }
