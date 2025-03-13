import os
import uuid
import re
from typing import List

from dotenv import load_dotenv
from fastapi import FastAPI
from faker import Faker
from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
from kafka.producer import KafkaProducer

from contextlib import asynccontextmanager
from commands import CreatePeopleCommand
from entities import Person

load_dotenv(verbose=True)

app = FastAPI()

faker = Faker()

@asynccontextmanager
async def lifespan(app: FastAPI):
    client = KafkaAdminClient(bootstrap_servers=os.getenv('BOOTSTRAP_SERVERS'))
    topic = NewTopic(
        name=os.environ['TOPICS_PEOPLE_BASIC_NAME'],
        num_partitions=int(os.environ['TOPICS_PEOPLE_BASIC_PARTITIONS']),
        replication_factor=int(os.environ['TOPICS_PEOPLE_BASIC_REPLICAS'])
    )
    print(f"Attempting to create topic: {topic.name}")
    try:
        client.create_topics([topic])
        print(f"Topic {topic.name} created successfully")
    except TopicAlreadyExistsError:
        print(f"Topic {topic.name} already exists")
    except Exception as e:
        print(f"Error creating topic: {e}")
    finally:
        client.close()
    yield

def make_producer():
    return KafkaProducer(bootstrap_servers=os.environ['BOOTSTRAP_SERVERS'])

@app.post('/api/people', status_code=201, response_model=List[Person])
async def create_people(cmd: CreatePeopleCommand):
    people: List[Person] = []

    faker = Faker()
    producer = make_producer()


    for _ in range(cmd.count):
        person = Person(id=str(uuid.uuid4()), name=faker.name(), title=faker.job().title())
        people.append(person)
        producer.send(topic=os.environ['TOPICS_PEOPLE_BASIC_NAME'],
                      key = re.sub(r'\s+', '-', person.title.lower()).encode('utf-8'),
                      value=person.model_dump_json().encode('utf-8'))
        
    producer.flush()

    return people