import requests
import json

# Schema Registry URL
SCHEMA_REGISTRY_URL = "http://localhost:8081"

# Dictionary of schema subjects and corresponding Avro schemas
schemas = {
    "ecommerce.orders-value": """
    {
        "namespace": "orders_version_1",
        "name": "orders",
        "type": "record",
        "fields": [
            {"name": "order_id", "type": "int"},
            {"name": "order_date", "type": "string"},
            {"name": "order_customer_id", "type": "int"},
            {"name": "order_status", "type": "string"}
        ]
    }
    """,
    "ecommerce.order_items-value": """
    {
        "namespace": "order_items_version_1",
        "name": "order_items",
        "type": "record",
        "fields": [
            {"name": "order_item_id", "type": "int"},
            {"name": "order_item_order_id", "type": "int"},
            {"name": "order_item_product_id", "type": "int"},
            {"name": "order_item_quantity", "type": "int"},
            {"name": "order_item_subtotal", "type": "float"},
            {"name": "order_item_product_price", "type": "float"}
        ]
    }
    """,
    "ecommerce.categories-value": """
    {
        "namespace": "categories_version_1",
        "name": "categories",
        "type": "record",
        "fields": [
            {"name": "category_id", "type": "int"},
            {"name": "category_department_id", "type": "int"},
            {"name": "category_name", "type": "string"}
        ]
    }
    """,
    "ecommerce.departments-value": """
    {
        "namespace": "departments_version_1",
        "name": "departments",
        "type": "record",
        "fields": [
            {"name": "department_id", "type": "int"},
            {"name": "department_name", "type": "string"}
        ]
    }
    """,
    "ecommerce.products-value": """
    {
        "namespace": "products_version_1",
        "name": "products",
        "type": "record",
        "fields": [
            {"name": "product_id", "type": "string"},
            {"name": "product_category_id", "type": "int"},
            {"name": "product_name", "type": "string"},
            {"name": "product_description", "type": ["null", "string"], "default": null},
            {"name": "product_price", "type": "float"},
            {"name": "product_image", "type": ["null", "string"], "default": null}
        ]
    }
    """,
    "ecommerce.customers-value": """
    {
        "namespace": "customers_version_1",
        "name": "customers",
        "type": "record",
        "fields": [
            {"name": "customer_id", "type": "int"},
            {"name": "customer_fname", "type": "string"},
            {"name": "customer_lname", "type": "string"},
            {"name": "customer_email", "type": "string"},
            {"name": "customer_password", "type": "string"},
            {"name": "customer_street", "type": "string"},
            {"name": "customer_city", "type": "string"},
            {"name": "customer_state", "type": "string"},
            {"name": "customer_zipcode", "type": "int"}
        ]
    }
    """
}

def register_schema(subject, schema):
    """Registers an Avro schema with the Confluent Schema Registry."""
    url = f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions"
    headers = {"Content-Type": "application/json"}
    payload = json.dumps({"schema": json.dumps(json.loads(schema))})  # Ensure valid JSON format
    
    response = requests.post(url, headers=headers, data=payload)
    
    if response.status_code == 200 or response.status_code == 201:
        print(f"✅ Successfully registered schema for: {subject}")
    else:
        print(f"❌ Failed to register schema for: {subject}. Error: {response.text}")

# Register all schemas
for subject, schema in schemas.items():
    register_schema(subject, schema)
