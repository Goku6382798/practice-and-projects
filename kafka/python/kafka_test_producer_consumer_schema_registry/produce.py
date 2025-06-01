import csv
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

# Kafka Configuration
bootstrap_servers = "localhost:9092"
topic = "mobile.logs.avro"

# Schema Registry Configuration
schema_registry_url = "http://localhost:8081"
schema_registry_client = SchemaRegistryClient({"url": schema_registry_url})

# Define Avro Schema
schema_str = """
{
    "type": "record",
    "name": "MobileLog",
    "fields": [
        {"name": "hour", "type": "string"},
        {"name": "lat", "type": "float"},
        {"name": "long", "type": "float"},
        {"name": "signal", "type": "int"},
        {"name": "network", "type": "string"},
        {"name": "operator", "type": "string"},
        {"name": "status", "type": "int"},
        {"name": "description", "type": "string"},
        {"name": "speed", "type": "float"},
        {"name": "satellites", "type": "int"},
        {"name": "precission", "type": "int"},
        {"name": "provider", "type": "string"},
        {"name": "activity", "type": "string"},
        {"name": "postal_code", "type": "string"}
    ]
}
"""

# Initialize Avro Serializer
avro_serializer = AvroSerializer(schema_registry_client, schema_str)

# Initialize Kafka Producer
producer = Producer({"bootstrap.servers": bootstrap_servers})

# CSV File Path
csv_file = "/home/gautam/practice/kafka_avro_people_service_v2/data/mobile-logs.csv"

# Read CSV and Produce Messages
with open(csv_file, mode="r") as file:
    reader = csv.DictReader(file, delimiter=",")  # Assuming CSV uses commas

    for row in reader:
        try:
            # Convert and sanitize values
            avro_data = avro_serializer(
                {
                    "hour": row["hour"],
                    "lat": float(row["lat"]),
                    "long": float(row["long"]),
                    "signal": int(row["signal"]),
                    "network": row["network"],
                    "operator": row["operator"],
                    "status": int(row["status"]),
                    "description": row["description"],
                    "speed": float(row["speed"]),
                    "satellites": int(float(row["satellites"])),  # ✅ Handle float values safely
                    "precission": int(float(row["precission"])),  # ✅ Handle float values safely
                    "provider": row["provider"],
                    "activity": row["activity"],
                    "postal_code": row["postal_code"]
                },
                SerializationContext(topic, MessageField.VALUE)  # ✅ Proper serialization context
            )

            # Send to Kafka
            producer.produce(topic, value=avro_data)
            print(f"✅ Sent: {row}")

        except Exception as e:
            print(f"❌ Error processing row {row}: {e}")

# Flush Kafka Producer
producer.flush()
