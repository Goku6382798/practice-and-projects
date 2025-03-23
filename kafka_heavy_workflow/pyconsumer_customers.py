import logging
import os
import json
import pandas as pd
from dotenv import load_dotenv
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
from models import customers
from schemas import CUSTOMERS_SCHEMA

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

load_dotenv(verbose=True)

consumed_logs = []
BATCH_SIZE = 1000
LOG_DIR = '/home/gautam/practice/kafka_heavy_workflow/customers'
FILE_PREFIX = "customers_logs_batch"

os.makedirs(LOG_DIR, exist_ok=True)

def save_logs():
    if not consumed_logs:
        logger.info("No logs to save")
        return
    
    existing_files = [f for f in os.listdir(LOG_DIR) if f.startswith(FILE_PREFIX) and f.endswith(".csv")]
    file_index = len(existing_files) + 1
    log_file_path = os.path.join(LOG_DIR, f"{FILE_PREFIX}_{file_index}.csv")
    df = pd.DataFrame(consumed_logs)
    df.to_csv(log_file_path, index=False)

    logger.info(f"Saved {len(consumed_logs)} logs to {log_file_path}")
    consumed_logs.clear()

def make_consumer():
    schema_reg_client = SchemaRegistryClient({'url': os.getenv('SCHEMA_REGISTRY_URL')})
    avro_deserializer = AvroDeserializer(
        schema_registry_client=schema_reg_client,
        schema_str=CUSTOMERS_SCHEMA,
        from_dict=lambda data, ctx: customers(**data)
    )
    string_deserializer = StringDeserializer('utf-8')

    return DeserializingConsumer({
        "bootstrap.servers": os.getenv("BOOTSTRAP_SERVERS"),
        "group.id": os.getenv("CONSUMER_GROUP"),
        "key.deserializer": string_deserializer,
        "value.deserializer": avro_deserializer,
        "auto.offset.reset": "earliest",
    })

def consume_messages():
    consumer = make_consumer()
    consumer.subscribe(["ecommerce.customers-value"])
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
                log_dict = log if isinstance(log, dict) else json.loads(log.decode('utf-8')) if isinstance(log, bytes) else log.__dict__
                
                consumed_logs.append(log_dict)
                logger.info(f"Consumed log: {log_dict}")

                if len(consumed_logs) >= BATCH_SIZE:
                    save_logs()
            except Exception as e:
                logger.error(f"Error deserializing message: {e}")

    except KeyboardInterrupt:
        logger.info("Stopping consumer...")
    finally:
        consumer.close()
        save_logs()

if __name__ == '__main__':
    consume_messages()
