# Checks and create topics in Kafka
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import logging

class KafkaTopicManager:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)

    def topic_exists(self, topic_name):
        existing_topics = self.admin_client.list_topics()
        return topic_name in existing_topics
    
    def  create_topic_if_missing(self,topic_name, num_partitions=1, replication_factor=1):
        if not self.topic_exists(topic_name):
            try:
                topic=NewTopic(
                    name=topic_name,
                    num_partitions=num_partitions,
                    replication_factor=replication_factor
                )
                self.admin_client.create_topics([topic])
                logging.info(f"[Kafka] Created topic '{topic_name}'")
            except TopicAlreadyExistsError:
                logging.warning(f"[Kafka] Topic already exists: {topic_name}")
            except Exception as e:
                logging.error(f"Failed to create topic '{topic_name}': {e}")
        else:
            logging.debug(f"[Kafka] Topic already exists: {topic_name}")