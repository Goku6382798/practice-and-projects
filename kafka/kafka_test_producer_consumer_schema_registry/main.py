import logging
import os
import csv
from time import sleep
from typing import List
from dotenv import load_dotenv
from fastapi import FastAPI
from contextlib import asynccontextmanager
from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka import SerializingProducer
from models import MobileLog
import schemas

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv(verbose=True)


def delivery_report(err, msg):
    """Handles Kafka message delivery reports."""
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(f"Message delivered to {msg.topic()} [partition {msg.partition()}]")


def make_producer():
    """Creates and returns a Kafka producer with Avro serialization."""
    schema_reg_client = SchemaRegistryClient({'url': os.getenv('SCHEMA_REGISTRY_URL', 'http://localhost:8081')})
    avro_serializer = AvroSerializer(
        schema_registry_client=schema_reg_client,
        schema_str=schemas.mobile_log_schema,
        to_dict=lambda log, ctx: log.model_dump()
    )
    return SerializingProducer({
        'bootstrap.servers': os.getenv('BOOTSTRAP_SERVERS', 'localhost:9092'),
        'linger.ms': 300,
        'enable.idempotence': True,
        'acks': 'all',
        'key.serializer': StringSerializer('utf_8'),
        'value.serializer': avro_serializer,
        'on_delivery': delivery_report
    })



def send_logs_to_kafka():
    """Reads CSV file and sends logs to Kafka in batches to prevent overload."""
    file_path = "data/mobile-logs.csv"
    producer = make_producer()
    logs = []
    batch_size = 100  # 🔹 Send logs in batches of 100

    if not os.path.exists(file_path):
        logger.error(f"❌ CSV file not found: {file_path}")
        return

    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                try:
                    # 🔹 Ensure postal_code is handled correctly
                    row["postal_code"] = float(row["postal_code"]) if row["postal_code"].strip() else None
                    log = MobileLog(**row)
                    logs.append(log)
                    producer.produce(
                        topic=os.getenv("TOPICS_MOBILE_LOGS_NAME", "mobile_logs"),
                        key=str(log.hour),
                        value=log,
                    )

                    # 🔹 Flush producer every batch_size messages
                    if len(logs) % batch_size == 0:
                        producer.flush()
                        logger.info(f"✅ Flushed {len(logs)} logs to Kafka...")
                        sleep(0.5)  # 🔹 Short delay to prevent overload

                except Exception as e:
                    logger.error(f"❌ Error processing row {row}: {e}")

        producer.flush()
        logger.info(f"✅ Successfully sent {len(logs)} logs to Kafka")
    except Exception as e:
        logger.error(f"❌ Error reading CSV: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Handles startup and shutdown events."""
    client = KafkaAdminClient(bootstrap_servers=os.getenv("BOOTSTRAP_SERVERS", "localhost:9092"))
    topic_name = os.getenv("TOPICS_MOBILE_LOGS_NAME", "mobile_logs")
    topic_partitions = int(os.getenv("TOPICS_MOBILE_LOGS_PARTITIONS", 3))
    topic_replicas = int(os.getenv("TOPICS_MOBILE_LOGS_REPLICAS", 1))

    topic = NewTopic(
        name=topic_name,
        num_partitions=topic_partitions,
        replication_factor=topic_replicas,
    )

    try:
        client.create_topics([topic])
        logger.info(f"✅ Kafka topic '{topic_name}' created successfully.")
    except TopicAlreadyExistsError:
        logger.warning(f"⚠️ Topic '{topic_name}' already exists.")
    except Exception as e:
        logger.error(f"❌ Error creating topic '{topic_name}': {e}")
    finally:
        client.close()

    # ✅ Send logs to Kafka when FastAPI starts
    logger.info("🚀 Sending logs to Kafka on startup...")
    send_logs_to_kafka()

    yield


# Initialize FastAPI with lifespan event
app = FastAPI(lifespan=lifespan)
