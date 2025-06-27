import pandas as pd
import os
import sys

# Add project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from kafka_app.producer.dma_worker import DMAProducerWorker
from kafka_app.producer.kafka_admin import KafkaTopicManager

def start_producers_from_csv(csv_file_path, interval_seconds=1):
    """
    Start producers for all DMAs in the CSV file
    """
    print(f"[Manager] Loading CSV: {csv_file_path}")
    
    try:
        # Read CSV file
        df = pd.read_csv(csv_file_path)
        print(f"[Manager] Loaded {len(df)} records from CSV")
        
        # Extract region from filename (e.g., "E1 2016_2017.csv" -> "e1")
        region = os.path.basename(csv_file_path).split()[0].lower()
        print(f"[Manager] Detected region: {region}")
        
        # Create Kafka topic manager
        kafka_manager = KafkaTopicManager()
        
        # Group by DMA
        dma_groups = df.groupby('DMA')
        print(f"[Manager] Found {len(dma_groups)} unique DMAs")
        
        workers = []
        
        for dma_id, dma_df in dma_groups:
            topic_name = f"dma.{dma_id}"
            print(f"[Manager] Creating topic: {topic_name}")
            
            # Create topic if it doesn't exist
            kafka_manager.create_topic_if_missing(topic_name)
            
            # Create and start worker
            worker = DMAProducerWorker(
                dma_id=dma_id,
                dma_df=dma_df,
                topic_name=topic_name,
                region=region,
                interval_seconds=interval_seconds
            )
            
            workers.append(worker)
            worker.start()
        
        print(f"[Manager] Started {len(workers)} producer workers")
        
        # Wait for all workers to complete
        for worker in workers:
            worker.join()
        
        print("[Manager] All producers finished")
        
    except Exception as e:
        print(f"[Manager] ERROR: {e}")
        raise

if __name__ == "__main__":
    # Start producers for E1 data
    start_producers_from_csv('data/E1 2016_2017.csv', interval_seconds=5)

