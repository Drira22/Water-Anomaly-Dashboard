from kafka import KafkaConsumer
from kafka_app.consumer.utils import parse_kafka_message, extract_region_from_message, validate_message
from kafka_app.consumer.db import create_table_if_missing, insert_messages, get_historical_data, insert_forecast
from kafka_app.consumer.config import KAFKA_CONFIG
from kafka_app.forecasting.model_service import forecasting_service 
import time
import pandas as pd
from collections import defaultdict
from datetime import datetime, timedelta

BATCH_SIZE = 1
BATCH_TIMEOUT = 1  # seconds

def generate_forecast_timestamps(start_timestamp):
    """Generate 96 timestamps for next 24 hours (15-minute intervals)"""
    timestamps = []
    current = pd.to_datetime(start_timestamp)
    
    for i in range(96):  # 96 points = 24 hours × 4 points/hour
        timestamps.append(current + timedelta(minutes=i * 15))
    
    return timestamps

def should_trigger_forecast(timestamp_str):
    """Check if timestamp is exactly 00:00:00 (start of new day)"""
    try:
        dt = pd.to_datetime(timestamp_str)
        return dt.hour == 0 and dt.minute == 0 and dt.second == 0
    except:
        return False

def main():
    print("[Consumer] Starting DMA consumer with forecasting...")
    consumer = KafkaConsumer(**{
        k: v for k, v in KAFKA_CONFIG.items() if k != 'topic_pattern'
    })
    consumer.subscribe(pattern=KAFKA_CONFIG['topic_pattern'])
    consumer.poll(timeout_ms=1000)  # force metadata update
    print("[Consumer] Subscribed to topics (after poll):", consumer.subscription())

    batches = defaultdict(list)
    last_flush_time = time.time()

    try:
        for raw_msg in consumer:
            try:
                msg = parse_kafka_message(raw_msg)
                print(f"[Consumer] Received message: {msg}")
                
                if not validate_message(msg):
                    print(f"[Consumer] Skipping invalid message: {msg}")
                    continue

                region = extract_region_from_message(msg)
                create_table_if_missing(region)

                # ===== NEW FORECASTING LOGIC =====
                if should_trigger_forecast(msg['timestamp']):
                    print(f"🔮 [Forecast] New day detected at {msg['timestamp']} for DMA {msg['dma_id']}")
                    
                    # Check if model is ready
                    if forecasting_service.is_model_ready():
                        # Get historical data (last 672 records)
                        historical_data = get_historical_data(region, msg['dma_id'], 672)
                        
                        if len(historical_data) >= 672:
                            print(f"[Forecast] Found {len(historical_data)} historical records. Generating forecast...")
                            
                            # Generate forecast
                            forecast_values = forecasting_service.forecast_next_day(historical_data)
                            
                            if forecast_values:
                                # Generate timestamps for forecast (next 24 hours)
                                forecast_timestamps = generate_forecast_timestamps(msg['timestamp'])
                                
                                # Prepare forecast data for database
                                forecast_data = list(zip(forecast_timestamps, forecast_values))
                                forecast_date = pd.to_datetime(msg['timestamp'], dayfirst=True).date()
                                
                                # Save forecast to database
                                insert_forecast(region, msg['dma_id'], forecast_date, forecast_data)
                                
                                print(f"✅ [Forecast] Saved {len(forecast_values)} predictions for DMA {msg['dma_id']}")
                            else:
                                print(f"❌ [Forecast] Failed to generate forecast for DMA {msg['dma_id']}")
                        else:
                            print(f"⚠️ [Forecast] Not enough historical data for DMA {msg['dma_id']} ({len(historical_data)}/672)")
                    else:
                        print("❌ [Forecast] Model not ready!")
                # ===== END FORECASTING LOGIC =====

                # Continue with normal message processing
                batches[region].append(msg)

                # Flush by size
                if len(batches[region]) >= BATCH_SIZE:
                    insert_messages(region, batches[region])
                    print(f"[Consumer] Flushed {len(batches[region])} records to '{region}'")
                    batches[region].clear()

                # Flush by timeout
                if time.time() - last_flush_time >= BATCH_TIMEOUT:
                    for region, msgs in batches.items():
                        if msgs:
                            insert_messages(region, msgs)
                            print(f"[Consumer] [Timer] Flushed {len(msgs)} to '{region}'")
                            batches[region].clear()
                    last_flush_time = time.time()

            except Exception as e:
                print(f"[Consumer] ERROR while processing message: {e}")

    finally:
        # Flush any remaining messages before exiting
        print("[Consumer] Final flush before shutdown...")
        for region, msgs in batches.items():
            if msgs:
                insert_messages(region, msgs)
                print(f"[Consumer] [Final Flush] Flushed {len(msgs)} to '{region}'")
                batches[region].clear()
        print("[Consumer] Kafka consumer loop exited.")

if __name__ == "__main__":
    main()