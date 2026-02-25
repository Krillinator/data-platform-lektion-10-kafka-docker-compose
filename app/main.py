import json

from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager

from kafka import KafkaProducer
from psycopg_pool import ConnectionPool
from psycopg.rows import dict_row

import os

from starlette import status

from app.schema.product import ProductSchema, ProductRead

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# Kafka Setup
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
PRODUCTS_TOPIC = os.getenv("PRODUCTS_TOPIC", "products.created")

DATABASE_URL = (
    f"postgresql://{DB_USER}:{DB_PASSWORD}"
    f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    app.state.pool = ConnectionPool(DATABASE_URL)

    with app.state.pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS products (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    price NUMERIC NOT NULL,
                    quantity INTEGER NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
            """)
        conn.commit()

    # Kafka producer (sync)
    app.state.kafka_producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )

    yield

    # Shutdown
    try:
        app.state.kafka_producer.close()
    except Exception:
        pass

    app.state.pool.close()


app = FastAPI(lifespan=lifespan)


@app.post("/products", status_code=status.HTTP_201_CREATED, response_model=ProductRead)
def post_product(product: ProductSchema):

    with app.state.pool.connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                INSERT INTO products (name, price, quantity)
                VALUES (%s, %s, %s)
                RETURNING id, name, price, quantity, created_at
                """,
                (product.name, product.price, product.quantity),
            )
            row = cur.fetchone()
            conn.commit()

    # Publish "product.created" event to Kafka
    event = {
        "type": "product.created",
        "product_id": row["id"],
        "name": row["name"],
        "price": str(row["price"]),
        "quantity": row["quantity"],
        "created_at": row["created_at"].isoformat(),
    }

    app.state.kafka_producer.send(
        PRODUCTS_TOPIC,
        key=str(row["id"]),
        value=event,
    )

    return row

@app.get("/products/{product_id}", response_model=ProductRead)
def get_product(product_id: int):

    with app.state.pool.connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT id, name, price, quantity, created_at
                FROM products
                WHERE id = %s
                """,
                (product_id,),
            )
            row = cur.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Product not found")

    return row

@app.get("/products", response_model=list[ProductRead])
def get_products(page: int = 1):

    limit = 10
    offset = (page - 1) * limit

    with app.state.pool.connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT id, name, price, quantity, created_at
                FROM products
                ORDER BY id
                LIMIT %s OFFSET %s
                """,
                (limit, offset),
            )
            rows = cur.fetchall()

    return rows