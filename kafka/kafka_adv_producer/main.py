import logging
import os
import uuid
import re
from typing import List, AsyncGenerator

from dotenv import load_dotenv
from fastapi import FastAPI
from contextlib import asynccontextmanager
from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
from kafka.producer import KafkaProducer
from pydantic import BaseModel
from faker import Faker

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# Load environment variables
load_dotenv(verbose=True)

# Define FastAPI app
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator:
    """Handles the startup and shutdown logic for the app."""
    client = KafkaAdminClient(bootstrap_servers=os.environ['BOOTSTRAP_SERVERS'])
    try:
        topic = NewTopic(
            name=os.environ['TOPICS_PEOPLE_ADV_NAME'],
            num_partitions=int(os.environ['TOPICS_PEOPLE_ADV_PARTITIONS']),
            replication_factor=int(os.environ['TOPICS_PEOPLE_ADV_REPLICAS'])
        )
        client.create_topics([topic])
    except TopicAlreadyExistsError as e:
        logger.info(f"Topic already exists: {e}")
    finally:
        client.close()

    yield  # The app runs after this

app = FastAPI(lifespan=lifespan)

# Kafka Producer Factory
def make_producer() -> KafkaProducer:
    """Creates a Kafka producer instance."""
    return KafkaProducer(
        bootstrap_servers=os.environ['BOOTSTRAP_SERVERS'],
        linger_ms=int(os.environ['TOPICS_PEOPLE_ADV_LINGER_MS']),
        retries=int(os.environ['TOPICS_PEOPLE_ADV_RETRIES']),
        max_in_flight_requests_per_connection=int(os.environ['TOPICS_PEOPLE_ADV_INFLIGHT_REQS']),
        acks=os.environ['TOPICS_PEOPLE_ADV_ACK']
    )

# Kafka Callbacks
class SuccessHandler:
    """Handles successful Kafka message delivery."""
    def __init__(self, person: "Person"):
        self.person = person

    def __call__(self, rec_metadata):
        logger.info(f"""
          Successfully produced person {self.person}
          to topic {rec_metadata.topic}
          and partition {rec_metadata.partition}
          at offset {rec_metadata.offset}""")

class ErrorHandler:
    """Handles Kafka message send failures."""
    def __init__(self, person: "Person"):
        self.person = person

    def __call__(self, ex):
        logger.error(f"Failed producing person {self.person}", exc_info=ex)

# Pydantic Models
class CreatePeopleCommand(BaseModel):
    count: int

class Person(BaseModel):
    id: str
    name: str
    title: str

# API Endpoint
@app.post('/api/people', status_code=201, response_model=List[Person])
async def create_people(cmd: CreatePeopleCommand):
    """Creates fake people and sends them to Kafka."""
    people: List[Person] = []
    faker = Faker()
    producer = make_producer()

    for _ in range(cmd.count):
        person = Person(id=str(uuid.uuid4()), name=faker.name(), title=faker.job().title())
        people.append(person)

        producer.send(
            topic=os.environ['TOPICS_PEOPLE_ADV_NAME'],
            key=re.sub(r"\s+", "-", person.title.lower()).encode('utf-8'),
            value=person.model_dump_json().encode('utf-8')  # Updated for Pydantic v2
        ).add_callback(SuccessHandler(person)).add_errback(ErrorHandler(person))
    
    producer.flush()
    return people