import os
import csv
import logging
from time import sleep
from dotenv import load_dotenv
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
from contextlib import asynccontextmanager
from fastapi import FastAPI
import models
from schema_registry import schemas, register_schema  # Import schemas and register_schema

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv(verbose=True)

BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC_NAME = os.getenv("TOPICS_NAME", "ecommerce.data.avro")
TOPIC_PARTITIONS = int(os.getenv("TOPICS_PARTITIONS", 3))
TOPIC_REPLICAS = int(os.getenv("TOPICS_REPLICAS", 1))
SCHEMA_REGISTRY_URL = os.getenv('SCHEMA_REGISTRY_URL', 'http://localhost:8081')

def delivery_report(err, msg):
    """Handles Kafka message delivery reports."""
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(f"Message delivered to {msg.topic()} [partition {msg.partition()}]")

def create_kafka_topic():
    """Creates Kafka topic if it doesn't exist."""
    try:
        client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
        topic = NewTopic(
            name=TOPIC_NAME,
            num_partitions=TOPIC_PARTITIONS,
            replication_factor=TOPIC_REPLICAS,
        )
        
        try:
            client.create_topics([topic])
            logger.info(f"✅ Kafka topic '{TOPIC_NAME}' created successfully.")
        except TopicAlreadyExistsError:
            logger.warning(f"⚠️ Topic '{TOPIC_NAME}' already exists.")
        except Exception as e:
            logger.error(f"❌ Error creating topic '{TOPIC_NAME}': {e}")
    except Exception as e:
        logger.error(f"❌ Failed to connect to Kafka broker: {e}")
    finally:
        client.close()

def make_producer(schema_str, subject_name):
    """Creates a Kafka producer with Avro serialization."""
    try:
        schema_reg_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})
    except Exception as e:
        logger.error(f"❌ Failed to connect to Schema Registry: {e}")
        return None
    
    avro_serializer = AvroSerializer(
        schema_registry_client=schema_reg_client,
        schema_str=schema_str,
        to_dict=lambda log, ctx: log.model_dump()
    )
    
    return SerializingProducer({
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'linger.ms': 0,  # Send messages immediately
        'batch.num.messages': 1,  # No batching
        'enable.idempotence': True,
        'acks': 'all',
        'key.serializer': StringSerializer('utf_8'),
        'value.serializer': avro_serializer,
        'on_delivery': delivery_report
    })

def send_all_data_to_kafka():
    """Reads CSV files and sends data to Kafka immediately without batching."""
    data_files = {
        "orders": ("retail_db/orders/part-00000", schemas["ecommerce.orders-value"], "ecommerce.orders-value"),
        "order_items": ("retail_db/order_items/part-00000", schemas["ecommerce.order_items-value"], "ecommerce.order_items-value"),
        "categories": ("retail_db/categories/part-00000", schemas["ecommerce.categories-value"], "ecommerce.categories-value"),
        "departments": ("retail_db/departments/part-00000", schemas["ecommerce.departments-value"], "ecommerce.departments-value"),
        "products": ("retail_db/products/part-00000", schemas["ecommerce.products-value"], "ecommerce.products-value"),
        "customers": ("retail_db/customers/part-00000", schemas["ecommerce.customers-value"], "ecommerce.customers-value"),
    }
    
    for table_name, (file_path, schema_str, topic_name) in data_files.items():
        if not os.path.exists(file_path):
            logger.warning(f"⚠️ No CSV file found: {file_path}")
            continue

        producer = make_producer(schema_str, topic_name)
        if producer is None:
            continue

        record_count = 0

        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    try:
                        model_class = getattr(models, table_name, None)
                        if model_class:
                            log = model_class(**row)

                            producer.produce(
                                topic=topic_name,
                                key=str(log.order_id) if hasattr(log, "order_id") else table_name,
                                value=log,
                            )
                            producer.flush()  # Send message immediately

                            record_count += 1
                        else:
                            logger.error(f"❌ No model found for {table_name}")
                    except Exception as e:
                        logger.error(f"❌ Error processing row {row} in {table_name}: {e}")
            
            logger.info(f"✅ Successfully sent {record_count} logs to Kafka for {table_name}")
        
        except Exception as e:
            logger.error(f"❌ Error reading CSV for {table_name}: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Handles startup and shutdown events."""
    create_kafka_topic()
    
    # Register schemas before sending data
    for subject, schema in schemas.items():
        register_schema(subject, schema)
    
    logger.info("🚀 Sending logs to Kafka on startup...")
    send_all_data_to_kafka()
    yield

# Initialize FastAPI with lifespan event
app = FastAPI(lifespan=lifespan)

if __name__ == "__main__":
    create_kafka_topic()  # Ensure topic exists before sending data
    # Register schemas before sending data
    for subject, schema in schemas.items():
        register_schema(subject, schema)
    send_all_data_to_kafka()
