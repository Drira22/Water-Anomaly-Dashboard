import asyncio
import asyncpg
from typing import List, Dict, Any
from datetime import datetime
import math

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

async def get_all_regions() -> List[str]:
    """Get all available regions from database (exclude forecast tables)"""
    conn = await get_connection()
    try:
        query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name LIKE 'flow_%'
        AND table_name NOT LIKE '%_forecast'
        """
        rows = await conn.fetch(query)
        regions = [row['table_name'].replace('flow_', '') for row in rows]
        return regions
    finally:
        await conn.close()

async def get_latest_flow_data(region: str, limit: int = 100) -> List[Dict[str, Any]]:
    """Get latest flow data for a region"""
    conn = await get_connection()
    try:
        table_name = f"flow_{region.lower()}"
        query = f"""
        SELECT id, dma_id, timestamp, flow 
        FROM {table_name} 
        WHERE flow IS NOT NULL AND flow != 'NaN'
        ORDER BY timestamp DESC 
        LIMIT $1
        """
        rows = await conn.fetch(query)
        result = []
        for row in rows:
            row_dict = dict(row)
            # Handle NaN values
            if math.isnan(row_dict['flow']) or row_dict['flow'] is None:
                row_dict['flow'] = 0.0
            result.append(row_dict)
        return result
    finally:
        await conn.close()

async def get_flow_data_by_time_range(region: str, start_time: str, end_time: str) -> List[Dict[str, Any]]:
    """Get flow data for a specific time range"""
    conn = await get_connection()
    try:
        table_name = f"flow_{region.lower()}"
        query = f"""
        SELECT id, dma_id, timestamp, flow 
        FROM {table_name} 
        WHERE timestamp BETWEEN $1 AND $2
        ORDER BY timestamp ASC
        """
        rows = await conn.fetch(query, start_time, end_time)
        return [dict(row) for row in rows]
    finally:
        await conn.close()

async def get_forecast_data(region: str, dma_id: str, forecast_date: str) -> List[Dict[str, Any]]:
    """Get forecast data for a specific DMA and date"""
    conn = await get_connection()
    try:
        table_name = f"flow_{region.lower()}_forecast"
        
        print(f"[DEBUG] Querying forecast table: {table_name}")
        print(f"[DEBUG] DMA ID: {dma_id}, Forecast Date: {forecast_date}")
        
        # Convert string date to Python date object
        from datetime import datetime
        date_obj = datetime.strptime(forecast_date, '%Y-%m-%d').date()
        
        query = f"""
        SELECT forecast_timestamp, forecasted_flow 
        FROM {table_name} 
        WHERE dma_id = $1 AND forecast_date = $2
        ORDER BY forecast_timestamp ASC
        """
        
        rows = await conn.fetch(query, str(dma_id), date_obj)
        print(f"[DEBUG] Found {len(rows)} forecast records")
        
        result = []
        for row in rows:
            result.append({
                'timestamp': row['forecast_timestamp'], 
                'flow': float(row['forecasted_flow'])
            })
        
        return result
    except Exception as e:
        print(f"[ERROR] get_forecast_data failed: {e}")
        return []
    finally:
        await conn.close()

async def get_available_forecast_dates(region: str, dma_id: str) -> List[str]:
    """Get available forecast dates for a DMA"""
    conn = await get_connection()
    try:
        table_name = f"flow_{region.lower()}_forecast"
        
        print(f"[DEBUG] Getting forecast dates from: {table_name}")
        
        query = f"""
        SELECT DISTINCT forecast_date 
        FROM {table_name} 
        WHERE dma_id = $1
        ORDER BY forecast_date DESC
        """
        
        rows = await conn.fetch(query, str(dma_id))
        dates = [str(row['forecast_date']) for row in rows]
        
        print(f"[DEBUG] Available forecast dates: {dates}")
        return dates
    except Exception as e:
        print(f"[ERROR] get_available_forecast_dates failed: {e}")
        return []
    finally:
        await conn.close()