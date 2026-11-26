
SCHEMA_REGISTRY_CONFIG = {
       'url': 'http://schema-registry:8081'
    }


AVRO_SCHEMA ="""{
            "type": "record",
            "name": "application",
            "namespace": "com.mbaranov",
            "fields": [
                {"name": "order_id", "type": "int"},
                {"name": "subject", "type": "string"}
            ]
            }
            """