ORDER_SCHEMA = """
{
    "type": "record",
    "name": "orders",
    "namespace": "orders_version_1",
    "fields": [
        {"name": "order_id", "type": "int"},
        {"name": "order_date", "type": "string"},
        {"name": "order_customer_id", "type": "int"},
        {"name": "order_status", "type": "string"}
    ]
}
"""

ORDER_ITEMS_SCHEMA = """
{
    "type": "record",
    "name": "order_items",
    "namespace": "orders_version_1",
    "fields": [
        {"name": "order_item_id", "type": "int"},
        {"name": "order_item_order_id", "type": "int"},
        {"name": "order_item_product_id", "type": "int"},
        {"name": "order_item_quantity", "type": "int"},
        {"name": "order_item_subtotal", "type": "float"},
        {"name": "order_item_product_price", "type": "float"}
    ]
}
"""

CATEGORIES_SCHEMA = """
{
    "type": "record",
    "name": "categories",
    "namespace": "orders_version_1",
    "fields": [
        {"name": "category_id", "type": "int"},
        {"name": "category_department_id", "type": "int"},
        {"name": "category_name", "type": "string"}
    ]
}
"""

DEPARTMENTS_SCHEMA = """
{
    "type": "record",
    "name": "departments",
    "namespace": "orders_version_1",
    "fields": [
        {"name": "department_id", "type": "int"},
        {"name": "department_name", "type": "string"}
    ]
}
"""

PRODUCTS_SCHEMA = """
{
    "type": "record",
    "name": "products",
    "namespace": "orders_version_1",
    "fields": [
        {"name": "product_id", "type": "string"},
        {"name": "product_category_id", "type": "int"},
        {"name": "product_name", "type": "string"},
        {"name": "product_description", "type": ["null", "string"], "default": null},
        {"name": "product_price", "type": "float"},
        {"name": "product_image", "type": ["null", "string"], "default": null}
    ]
}
"""

CUSTOMERS_SCHEMA = """
{
    "type": "record",
    "name": "customers",
    "namespace": "orders_version_1",
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