from fastapi import FastAPI, HTTPException, BackgroundTasks, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Optional, Dict
import asyncio
import subprocess
import psutil
import signal
import os
import math
from datetime import datetime, timedelta
from .db import get_latest_flow_data, get_flow_data_by_time_range, get_all_regions, get_forecast_data, get_available_forecast_dates
from .kafka_controller import KafkaController
import uvicorn
import asyncpg
from .websocket_manager import manager 
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from pydantic import BaseModel



app = FastAPI(title="Water Flow Monitoring API", version="1.0.0")

# Add CORS middleware for Streamlit
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # or restrict to ["http://localhost:5173"]
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
# Add these new endpoints to your existing main.py

@app.get("/forecast-data/{region}/{dma_id}")
async def get_dma_forecast_data(region: str, dma_id: str, forecast_date: Optional[str] = None):
    """Get forecast data for a specific DMA and date"""
    try:
        if not forecast_date:
            # Get latest forecast date
            dates = await get_available_forecast_dates(region, dma_id)
            if not dates:
                return {"success": True, "data": []}
            forecast_date = dates[0]
        
        data = await get_forecast_data(region, dma_id, forecast_date)
        return {"success": True, "data": data, "forecast_date": forecast_date}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/forecast-dates/{region}/{dma_id}")
async def get_forecast_dates(region: str, dma_id: str):
    """Get available forecast dates for a DMA"""
    try:
        dates = await get_available_forecast_dates(region, dma_id)
        return {"success": True, "dates": dates}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.websocket("/ws/flow/{region}/{dma_id}")
async def flow_websocket(websocket: WebSocket, region: str, dma_id: str):
    await websocket.accept()  # ✅ Required to establish connection
    print(f"[WebSocket] ✅ New connection: {region}_{dma_id}")
    await manager.connect(websocket, region, dma_id)
    try:
        while True:
            message = await websocket.receive_text()
            print(f"[WebSocket] 🔁 Received from client: {message}")
    except WebSocketDisconnect:
        print(f"[WebSocket] ⚠️ Client disconnected from {region}_{dma_id}")
        await manager.disconnect(websocket, region, dma_id)

class BroadcastRequest(BaseModel):
    region: str
    dma_id: str
    timestamp: str
    flow: float

@app.post("/internal/broadcast")
async def internal_broadcast(data: BroadcastRequest):
    message = {
        "timestamp": data.timestamp,
        "flow": data.flow,
        "region": data.region,
        "dma_id": data.dma_id
    }
    try:
        await manager.broadcast(data.region, data.dma_id, message)
        print(f"[API Broadcast] ✅ Sent to {data.region}_{data.dma_id}")
        return {"success": True}
    except Exception as e:
        print(f"[API Broadcast] ❌ Failed to send: {e}")
        return {"success": False, "error": str(e)}
    
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)