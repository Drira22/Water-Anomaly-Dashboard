import psycopg2
from psycopg2.extras import execute_values
from kafka_app.consumer.config import POSTGRES_CONFIG as DB_CONFIG
from kafka_app.consumer.utils import get_table_name

# --- Connect to PostgreSQL ---
def get_connection():
    return psycopg2.connect(**DB_CONFIG)

# --- Create table for a region if it doesn't exist (WITH FORECAST TABLE) ---
def create_table_if_missing(region: str):
    table_name = get_table_name(region)
    forecast_table_name = f"{table_name}_forecast"
    
    # Main flow table
    create_flow_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        dma_id VARCHAR(50),
        timestamp TIMESTAMP,
        flow FLOAT
    );
    """
    
    # Forecast table (created dynamically alongside main table)
    create_forecast_sql = f"""
    CREATE TABLE IF NOT EXISTS {forecast_table_name} (
        id SERIAL PRIMARY KEY,
        dma_id VARCHAR(50),
        forecast_date DATE,
        forecast_timestamp TIMESTAMP,
        forecasted_flow FLOAT,
        created_at TIMESTAMP DEFAULT NOW()
    );
    """
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(create_flow_sql)
            cur.execute(create_forecast_sql)
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

# --- Insert forecast data into forecast table ---
def insert_forecast(region: str, dma_id: str, forecast_date: str, forecast_data: list):
    """
    Insert forecast data for a specific DMA and date
    forecast_data: list of tuples [(timestamp, forecasted_flow), ...]
    """
    table_name = get_table_name(region)
    forecast_table_name = f"{table_name}_forecast"
    
    insert_sql = f"""
    INSERT INTO {forecast_table_name} (dma_id, forecast_date, forecast_timestamp, forecasted_flow)
    VALUES %s;
    """
    
    values = [
        (dma_id, forecast_date, timestamp, flow) 
        for timestamp, flow in forecast_data
    ]
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            execute_values(cur, insert_sql, values)
        conn.commit()

# --- Get historical data for forecasting ---
def get_historical_data(region: str, dma_id: str, limit: int = 672):
    """
    Get last N records for a specific DMA (for model input)
    Returns: list of dicts with timestamp and flow
    """
    table_name = get_table_name(region)
    query = f"""
    SELECT timestamp, flow 
    FROM {table_name} 
    WHERE dma_id = %s AND flow IS NOT NULL
    ORDER BY timestamp DESC 
    LIMIT %s;
    """
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (dma_id, limit))
            rows = cur.fetchall()
            
    # Return in chronological order (oldest first)
    return [{'timestamp': row[0], 'flow': row[1]} for row in reversed(rows)]