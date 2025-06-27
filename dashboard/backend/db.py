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
    """Get all available regions from database"""
    conn = await get_connection()
    try:
        query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name LIKE 'flow_%'
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