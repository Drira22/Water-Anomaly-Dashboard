from kafka import KafkaConsumer
from kafka_app.consumer.utils import parse_kafka_message, extract_region_from_message, validate_message
from kafka_app.consumer.db import create_table_if_missing, insert_messages
from kafka_app.consumer.config import KAFKA_CONFIG

import time
from collections import defaultdict

BATCH_SIZE = 20
BATCH_TIMEOUT = 5  # seconds

def main():
    print("[Consumer] Starting DMA consumer...")

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
