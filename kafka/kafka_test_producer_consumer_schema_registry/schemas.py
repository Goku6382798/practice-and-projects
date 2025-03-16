mobile_log_schema = """
{
    "namespace": "com.thecodinginterface.avrodomainevents",
    "name": "MobileLog",
    "type": "record",
    "fields": [
        {"name": "hour", "type": "string"},
        {"name": "lat", "type": "double"},
        {"name": "long", "type": "double"},
        {"name": "signal", "type": "int"},
        {"name": "network", "type": "string"},
        {"name": "operator", "type": "string"},
        {"name": "status", "type": "int"},
        {"name": "description", "type": "string"},
        {"name": "speed", "type": "double"},
        {"name": "satellites", "type": "double"},
        {"name": "precission", "type": "string"},
        {"name": "provider", "type": "string"},
        {"name": "activity", "type": "string"},
        {"name": "postal_code", "type": ["null", "double"], "default": null}
    ]
}
"""