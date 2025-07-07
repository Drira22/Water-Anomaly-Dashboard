import json
import threading
import time
from kafka import KafkaProducer
import pandas as pd

class DMAProducerWorker(threading.Thread):
    def __init__(self, dma_id, dma_df, topic_name, region, interval_seconds=1, bootstrap_servers='localhost:9092'):
        super().__init__()
        self.dma_id = dma_id
        self.dma_df = dma_df
        self.topic_name = topic_name
        self.region = region.lower()
        self.interval_seconds = interval_seconds
        self.bootstrap_servers = bootstrap_servers

        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        # self.daemon = True  # You can leave this commented to avoid premature exit

    def run(self):
        print(f"[{self.dma_id}] Streaming {len(self.dma_df)} records to topic '{self.topic_name}' for region '{self.region}'")

        try:
            for idx, (_, row) in enumerate(self.dma_df.iterrows()):
                timestamp = row['Datetime'].strftime('%Y-%m-%d %H:%M:%S')
                message = {
                    'dma_id': str(row['DMA']),
                    'region': self.region,
                    'timestamp': timestamp,
                    'flow': float(row['Flow'])
                }

                self.producer.send(self.topic_name, value=message)
                print(f"[{self.dma_id}] Sent: {message}")

                if idx < len(self.dma_df) - 1:
                    print(f"[{self.dma_id}] Waiting {self.interval_seconds} seconds for next record...")
                    time.sleep(self.interval_seconds)

            self.producer.flush()
            print(f"[{self.dma_id}] Finished streaming.")

        except Exception as e:
            print(f"[{self.dma_id}] ERROR: {e}")

        finally:
            self.producer.close()