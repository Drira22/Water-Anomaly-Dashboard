# consumer/config.py
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC_PATTERN = 'dma.*'

POSTGRES_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'dbname': 'yorkshire',
    'user': 'yorkshire',
    'password': 'yorkshire'
}

KAFKA_CONFIG = {
    'bootstrap_servers': KAFKA_BOOTSTRAP_SERVERS,
    'auto_offset_reset': 'earliest',
    'enable_auto_commit': True,
    'group_id': 'dma-consumer-group',
    'value_deserializer': lambda m: m,
    'key_deserializer': lambda k: k.decode('utf-8') if k else None,
    'consumer_timeout_ms': 1000000,
    'topic_pattern': KAFKA_TOPIC_PATTERN,  # <<< add this
}

