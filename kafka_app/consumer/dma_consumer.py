from kafka import KafkaConsumer
from kafka_app.consumer.utils import parse_kafka_message, extract_region_from_message, validate_message
from kafka_app.consumer.db import create_table_if_missing, insert_messages, get_historical_data, insert_forecast
from kafka_app.consumer.config import KAFKA_CONFIG
from kafka_app.forecasting.model_service import forecasting_service
from dashboard.backend.websocket_manager import manager  # ✅ Use the new manager
import requests
import asyncio
import time
import pandas as pd
from collections import defaultdict
from datetime import timedelta

BATCH_SIZE = 1
BATCH_TIMEOUT = 6  # seconds

# Create a dedicated asyncio event loop
event_loop = asyncio.new_event_loop()
asyncio.set_event_loop(event_loop)

def generate_forecast_timestamps(start_timestamp):
    current = pd.to_datetime(start_timestamp)
    return [current + timedelta(minutes=15 * i) for i in range(96)]

def should_trigger_forecast(timestamp_str):
    try:
        dt = pd.to_datetime(timestamp_str)
        return dt.hour == 0 and dt.minute == 0 and dt.second == 0
    except:
        return False

def broadcast_via_api(region: str, dma_id: str, message: dict):
    try:
        response = requests.post(
            "http://localhost:8000/internal/broadcast",
            json={
                "region": region,
                "dma_id": dma_id,
                "timestamp": message["timestamp"],
                "flow": message["flow"]
            },
            timeout=2
        )
        if response.status_code == 200:
            print(f"[API Broadcast] ✅ Sent flow data to {region}_{dma_id}")
        else:
            print(f"[API Broadcast] ❌ Failed: {response.status_code} {response.text}")
    except Exception as e:
        print(f"[API Broadcast] ❌ Exception: {e}")

def broadcast_forecast_via_api(region: str, dma_id: str, forecast_data: list, forecast_date: str):
    """Broadcast forecast data via WebSocket"""
    try:
        response = requests.post(
            "http://localhost:8000/internal/broadcast-forecast",
            json={
                "region": region,
                "dma_id": dma_id,
                "forecast_date": forecast_date,
                "forecast_data": forecast_data
            },
            timeout=5
        )
        if response.status_code == 200:
            print(f"[Forecast Broadcast] ✅ Sent forecast to {region}_{dma_id}")
        else:
            print(f"[Forecast Broadcast] ❌ Failed: {response.status_code} {response.text}")
    except Exception as e:
        print(f"[Forecast Broadcast] ❌ Exception: {e}")

def main():
    print("[Consumer] Starting DMA consumer with forecasting + WebSocket...")
    # ✅ Wait a few seconds to allow frontend WebSocket clients to connect
    print("⏳ Waiting 3 seconds for WebSocket clients to connect...")
    time.sleep(3)

    consumer = KafkaConsumer(**{k: v for k, v in KAFKA_CONFIG.items() if k != 'topic_pattern'})
    consumer.subscribe(pattern=KAFKA_CONFIG['topic_pattern'])
    consumer.poll(timeout_ms=1000)
    print("[Consumer] Subscribed to topics:", consumer.subscription())

    batches = defaultdict(list)
    last_flush_time = time.time()

    try:
        asyncio.get_event_loop().run_in_executor(None, event_loop.run_forever)

        for raw_msg in consumer:
            try:
                msg = parse_kafka_message(raw_msg)
                print(f"[Consumer] Received: {msg}")

                if not validate_message(msg):
                    print(f"[Consumer] Invalid message skipped: {msg}")
                    continue

                region = extract_region_from_message(msg)
                create_table_if_missing(region)

                # === Forecasting ===
                if should_trigger_forecast(msg['timestamp']):
                    print(f"🔮 Forecast triggered for DMA {msg['dma_id']} at {msg['timestamp']}")
                    
                    if forecasting_service.is_model_ready():
                        historical = get_historical_data(region, msg['dma_id'], 672)
                        
                        if len(historical) >= 672:
                            forecasts = forecasting_service.forecast_next_day(historical)
                            
                            if forecasts:
                                future_timestamps = generate_forecast_timestamps(msg['timestamp'])
                                forecast_data = list(zip(future_timestamps, forecasts))
                                
                                # Save to database
                                insert_forecast(region, msg['dma_id'], pd.to_datetime(msg['timestamp']).date(), forecast_data)
                                print(f"✅ Forecast saved for {msg['dma_id']}")
                                
                                # 🚀 NEW: Broadcast forecast data via WebSocket
                                forecast_for_broadcast = [
                                    {
                                        "timestamp": ts.isoformat(),
                                        "flow": float(flow)
                                    }
                                    for ts, flow in forecast_data
                                ]
                                
                                broadcast_forecast_via_api(
                                    region, 
                                    msg['dma_id'], 
                                    forecast_for_broadcast,
                                    str(pd.to_datetime(msg['timestamp']).date())
                                )
                            else:
                                print(f"❌ Forecast failed for {msg['dma_id']}")
                        else:
                            print(f"⚠️ Not enough history ({len(historical)}/672) for {msg['dma_id']}")
                    else:
                        print("❌ Model not ready")

                # === Add to batch ===
                batches[region].append(msg)

                # === Flush by batch size ===
                if len(batches[region]) >= BATCH_SIZE:
                    insert_messages(region, batches[region])
                    print(f"[Consumer] Inserted {len(batches[region])} to DB ({region})")
                    
                    for m in batches[region]:
                        broadcast_via_api(region, m['dma_id'], {
                            "timestamp": pd.to_datetime(m["timestamp"]).isoformat(),
                            "flow": float(m["flow"]),
                            "region": region,
                            "dma_id": m["dma_id"]
                        })
                    
                    batches[region].clear()

                # === Flush by timeout ===
                if time.time() - last_flush_time >= BATCH_TIMEOUT:
                    for region, msgs in batches.items():
                        if msgs:
                            insert_messages(region, msgs)
                            print(f"[Consumer] [Timeout Flush] {len(msgs)} for {region}")
                            
                            for m in msgs:
                                broadcast_via_api(region, m['dma_id'], {
                                    "timestamp": pd.to_datetime(m["timestamp"]).isoformat(),
                                    "flow": float(m["flow"]),
                                    "region": region,
                                    "dma_id": m["dma_id"]
                                })
                            
                            batches[region].clear()
                    
                    last_flush_time = time.time()

            except Exception as e:
                print(f"[Consumer] ERROR: {e}")

    finally:
        print("[Consumer] Final flush before exit...")
        for region, msgs in batches.items():
            if msgs:
                insert_messages(region, msgs)
                for m in msgs:
                    broadcast_via_api(region, m['dma_id'], {
                        "timestamp": pd.to_datetime(m["timestamp"]).isoformat(),
                        "flow": float(m["flow"]),
                        "region": region,
                        "dma_id": m["dma_id"]
                    })
        
        print("[Consumer] Finished.")
        event_loop.stop()

if __name__ == "__main__":
    main()
