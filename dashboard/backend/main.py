from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Optional, Dict
import asyncio
import subprocess
import psutil
import signal
import os
import math
from datetime import datetime, timedelta
from .db import get_latest_flow_data, get_flow_data_by_time_range, get_all_regions
from .kafka_controller import KafkaController
import uvicorn
import asyncpg

app = FastAPI(title="Water Flow Monitoring API", version="1.0.0")

# Add CORS middleware for Streamlit
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'yorkshire',
    'user': 'yorkshire',
    'password': 'yorkshire'
}

async def get_connection():
    """Get database connection"""
    return await asyncpg.connect(**DB_CONFIG)

# Initialize Kafka controller
kafka_controller = KafkaController()

@app.get("/")
async def root():
    return {"message": "Water Flow Monitoring API is running"}

@app.get("/regions")
async def get_regions():
    """Get all available regions"""
    try:
        regions = await get_all_regions()
        return {"success": True, "regions": regions}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/dmas/{region}")
async def get_dmas_for_region(region: str):
    """Get all DMAs for a specific region"""
    try:
        conn = await get_connection()
        table_name = f"flow_{region.lower()}"
        query = f"SELECT DISTINCT dma_id FROM {table_name} ORDER BY dma_id"
        rows = await conn.fetch(query)
        dmas = [row['dma_id'] for row in rows]
        await conn.close()
        return {"success": True, "dmas": dmas}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/flow-data/{region}/{dma_id}")
async def get_dma_flow_data(region: str, dma_id: str, limit: Optional[int] = 10):
    """Get flow data for a specific DMA"""
    try:
        conn = await get_connection()
        table_name = f"flow_{region.lower()}"
        query = f"""
        SELECT id, dma_id, timestamp, flow 
        FROM {table_name} 
        WHERE dma_id = $1 AND flow IS NOT NULL
        ORDER BY timestamp DESC 
        LIMIT $2
        """
        rows = await conn.fetch(query, dma_id, limit)
        result = []
        for row in rows:
            row_dict = dict(row)
            # Handle NaN values
            if row_dict['flow'] is None or (isinstance(row_dict['flow'], float) and math.isnan(row_dict['flow'])):
                row_dict['flow'] = 0.0
            result.append(row_dict)
        await conn.close()
        return {"success": True, "data": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/flow-data/{region}")
async def get_flow_data(
    region: str, 
    limit: Optional[int] = 100,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None
):
    """Get flow data for a specific region"""
    try:
        if start_time and end_time:
            data = await get_flow_data_by_time_range(region, start_time, end_time)
        else:
            data = await get_latest_flow_data(region, limit)
        
        # Handle NaN values in the data
        cleaned_data = []
        for record in data:
            if record['flow'] is None or (isinstance(record['flow'], float) and math.isnan(record['flow'])):
                record['flow'] = 0.0
            cleaned_data.append(record)
        
        return {"success": True, "data": cleaned_data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ===== KAFKA CONTROL ENDPOINTS =====

@app.get("/kafka/status")
async def get_kafka_status():
    """Get status of Kafka producer and consumer"""
    try:
        status = kafka_controller.get_status()
        return {"success": True, "status": status}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/kafka/producer/start")
async def start_producer():
    """Start Kafka producer"""
    try:
        result = kafka_controller.start_producer()
        return {"success": True, "message": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/kafka/producer/stop")
async def stop_producer():
    """Stop Kafka producer"""
    try:
        result = kafka_controller.stop_producer()
        return {"success": True, "message": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/kafka/consumer/start")
async def start_consumer():
    """Start Kafka consumer"""
    try:
        result = kafka_controller.start_consumer()
        return {"success": True, "message": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/kafka/consumer/stop")
async def stop_consumer():
    """Stop Kafka consumer"""
    try:
        result = kafka_controller.stop_consumer()
        return {"success": True, "message": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/kafka/logs/{service}")
async def get_kafka_logs(service: str, lines: int = 50):
    """Get logs from Kafka producer or consumer"""
    try:
        logs = kafka_controller.get_logs(service, lines)
        return {"success": True, "logs": logs}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)