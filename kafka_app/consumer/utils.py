# consumer/utils.py
import re

def extract_region_from_message(message: dict) -> str:
    """
    Extract region (e.g., 'e1') from Kafka message payload.
    """
    region = message.get('region', 'unknown').lower()
    return region

def get_table_name(region: str) -> str:
    """
    Returns the PostgreSQL table name for a given region.
    Example: 'e1' -> 'flow_e1'
    """
    return f"flow_{region}"

def validate_message(message: dict) -> bool:
    """
    Simple schema check to make sure message has required fields.
    """
    return all(k in message for k in ['dma_id', 'region', 'timestamp', 'flow'])

def parse_kafka_message(raw_msg) -> dict:
    """
    Deserialize Kafka JSON message.
    """
    import json
    return json.loads(raw_msg.value.decode('utf-8'))
