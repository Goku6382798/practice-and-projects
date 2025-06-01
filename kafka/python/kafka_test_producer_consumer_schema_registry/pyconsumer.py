import logging
import os
import json
import pandas as pd
from dotenv import load_dotenv
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
from schemas import mobile_log_schema
from models import MobileLog

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# Load environment variables
load_dotenv(verbose=True)

# Global variables
consumed_logs = []
BATCH_SIZE = 1000  # Save after every 1000 records
LOG_DIR = "/home/gautam/practice/kafka_avro_people_service_v2/logs/"
FILE_PREFIX = "mobile_logs_batch"

# Ensure log directory exists
os.makedirs(LOG_DIR, exist_ok=True)

def save_logs():
    """Saves logs to a CSV file in batches of 1000."""
    if not consumed_logs:
        logger.info("No logs to save.")
        return

    # Determine file index for rolling logs
    existing_files = [f for f in os.listdir(LOG_DIR) if f.startswith(FILE_PREFIX) and f.endswith(".csv")]
    file_index = len(existing_files) + 1  # Increment file index

    # Save logs to a new CSV file
    log_file_path = os.path.join(LOG_DIR, f"{FILE_PREFIX}_{file_index}.csv")
    df = pd.DataFrame(consumed_logs)
    df.to_csv(log_file_path, index=False)

    logger.info(f"Saved {len(consumed_logs)} logs to {log_file_path}")
    consumed_logs.clear()  # Clear list after saving

def make_consumer():
    """Creates and returns a Kafka consumer with Avro deserialization."""
    schema_reg_client = SchemaRegistryClient({'url': os.getenv('SCHEMA_REGISTRY_URL')})
    avro_deserializer = AvroDeserializer(
        schema_registry_client=schema_reg_client,
        schema_str=mobile_log_schema,
        from_dict=lambda data, ctx: MobileLog(**data)
    )
    string_deserializer = StringDeserializer('utf_8')

    return DeserializingConsumer({
        "bootstrap.servers": os.getenv("BOOTSTRAP_SERVERS"),
        "group.id": os.getenv("CONSUMER_GROUP"),
        "key.deserializer": string_deserializer,
        "value.deserializer": avro_deserializer,
        "auto.offset.reset": "earliest",
    })

def consume_messages():
    """Consumes messages from Kafka and saves them in batches of 1000 to CSV."""
    consumer = make_consumer()
    consumer.subscribe([os.getenv('TOPICS_MOBILE_LOGS_NAME')])

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue
            try:
                log = msg.value()
                
                # Convert log to dictionary if it's a class object
                if isinstance(log, MobileLog):
                    log_dict = log.__dict__
                else:
                    log_dict = log if isinstance(log, dict) else json.loads(str(log))

                consumed_logs.append(log_dict)

                logger.info(f"Consumed log: {log_dict}")

                consumer.commit(message=msg)

                # Save logs if batch size reaches 1000
                if len(consumed_logs) >= BATCH_SIZE:
                    save_logs()

            except Exception as e:
                logger.error(f"Error deserializing message: {e}")
    except KeyboardInterrupt:
        logger.info("Stopping consumer...")
    finally:
        consumer.close()
        save_logs()  # Save remaining logs before exiting

if __name__ == '__main__':
    consume_messages()