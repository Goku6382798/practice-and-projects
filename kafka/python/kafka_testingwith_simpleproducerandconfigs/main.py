import logging
import os
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI
from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError

# Load environment variables
load_dotenv(verbose=True)

logger = logging.getLogger()

# Lifespan function to handle startup and shutdown
@asynccontextmanager
async def lifespan(app: FastAPI):
    client = KafkaAdminClient(bootstrap_servers=os.environ["BOOTSTRAP_SERVERS"])
    topic = NewTopic(
        name=os.environ["TOPICS_PEOPLE_BASIC_NAME"],
        num_partitions=int(os.environ["TOPICS_PEOPLE_BASIC_PARTITIONS"]),
        replication_factor=int(os.environ["TOPICS_PEOPLE_BASIC_REPLICAS"]),
    )

    try:
        client.create_topics([topic])
        logger.info("Kafka topic created successfully.")
    except TopicAlreadyExistsError:
        logger.warning("Topic already exists.")
    finally:
        client.close()

    yield  # Hand over control to the FastAPI app

# Create the FastAPI app with the lifespan context
app = FastAPI(lifespan=lifespan)

@app.get("/hello-world")
async def hello_world():
    return {"message": "Hello World!"}
