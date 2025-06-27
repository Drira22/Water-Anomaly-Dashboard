import psycopg2
from psycopg2.extras import execute_values
from kafka_app.consumer.config import POSTGRES_CONFIG as DB_CONFIG
from kafka_app.consumer.utils import get_table_name

# --- Connect to PostgreSQL ---
def get_connection():
    return psycopg2.connect(**DB_CONFIG)

# --- Create table for a region if it doesn't exist ---
def create_table_if_missing(region: str):
    table_name = get_table_name(region)
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        dma_id VARCHAR(50),
        timestamp TIMESTAMP,
        flow FLOAT
    );
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(create_sql)
        conn.commit()

# --- Insert batch of messages into region table ---
def insert_messages(region: str, messages: list[dict]):
    table_name = get_table_name(region)
    insert_sql = f"""
    INSERT INTO {table_name} (dma_id, timestamp, flow)
    VALUES %s;
    """
    values = [
        (msg['dma_id'], msg['timestamp'], msg['flow']) for msg in messages
    ]
    with get_connection() as conn:
        with conn.cursor() as cur:
            execute_values(cur, insert_sql, values)
        conn.commit()
