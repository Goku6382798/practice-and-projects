import logging
import os
import re
from typing import List

from dotenv import load_dotenv
from fastapi import FastAPI
from faker import Faker

from confluent_kafka import SerializingProducer
from confluent_kafka.admin import AdminClient, NewTopic

from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from contextlib import asynccontextmanager

import schemas
from commands import CreatePeopleCommand
from models import Person

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# Load environment variables
load_dotenv(verbose=True)

# Initialize FastAPI
app = FastAPI()


@asynccontextmanager
async def lifespan(app: FastAPI):
    client = AdminClient({'bootstrap.servers': os.getenv('BOOTSTRAP_SERVERS')})

    topic_name = os.getenv('TOPICS_PEOPLE_AVRO_NAME')
    partitions = int(os.getenv('TOPICS_PEOPLE_AVRO_PARTITIONS', 1))
    replicas = int(os.getenv('TOPICS_PEOPLE_AVRO_REPLICAS', 1))

    topic = NewTopic(topic_name, num_partitions=partitions, replication_factor=replicas)

    try:
        futures = client.create_topics([topic])
        for topic_name, future in futures.items():
            future.result()
            logger.info(f"Created topic {topic_name}")
    except Exception as e:
        logger.warning(f"Error creating topic: {e}")

    yield 

def make_producer() -> SerializingProducer:
    """Creates a Kafka producer with Avro serialization."""
    schema_reg_client = SchemaRegistryClient({'url': os.environ['SCHEMA_REGISTRY_URL']})
    
    avro_serializer = AvroSerializer(schema_reg_client,
                                     schemas.person_value_v2,
                                     lambda person, ctx: person.dict())

    return SerializingProducer({
        'bootstrap.servers': os.getenv('BOOTSTRAP_SERVERS'),
        'linger.ms': 300,
        'enable.idempotence': True,
        'max.in.flight.requests.per.connection': 1,
        'acks': 'all',
        'key.serializer': StringSerializer('utf_8'),
        'value.serializer': avro_serializer,
        'partitioner': 'murmur2_random'
    })

class ProducerCallback:
    """Handles Kafka message delivery callbacks."""
    def __init__(self, person):
        self.person = person

    def __call__(self, err, msg):
        if err:
            logger.error(f"Failed to produce {self.person}", exc_info=err)
        else:
            logger.info(f"Successfully produced {self.person} to partition {msg.partition()} at offset {msg.offset()}")

@app.post('/api/people', status_code=201, response_model=List[Person])
async def create_people(cmd: CreatePeopleCommand):
    people: List[Person] = []
    faker = Faker()
    producer = make_producer()

    for _ in range(cmd.count):
        person = Person(first_name=faker.first_name(), last_name=faker.last_name(), title=faker.job())
        people.append(person)

        key = re.sub(r'\s+', '-', person.title.lower())  # Correct regex usage

        producer.produce(
            topic=os.getenv('TOPICS_PEOPLE_AVRO_NAME'),
            key=key,
            value=person,
            on_delivery=ProducerCallback(person)
        )

    producer.flush()
    return people  # Returning the created people list