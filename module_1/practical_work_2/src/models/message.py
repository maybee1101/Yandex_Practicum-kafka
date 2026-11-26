

import logging
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from confluent_kafka.serialization import IntegerSerializer, IntegerDeserializer, SerializationContext, MessageField

logger = logging.getLogger(__name__)

class Message:
    def __init__(self, order_id: int = None, 
                       subject: str = None, 
                       topic_name: str = None, 
                       schema_registry_client: SchemaRegistryClient = None, 
                       avro_schema: str = None):
        
        self.order_id = order_id
        self.subject = subject
        self.topic_name = topic_name
        self.schema_registry_client = schema_registry_client
        self.avro_schema = avro_schema

    
                   
    def key_int_serialize(self) -> bytes:
        """
        Метод для сериализации ключа в формате int в байты
        
        Returns:
            bytes: байты, которые потом можно десериализовать обратно в int
        """
        try:
            key = self.order_id
            key_serializer = IntegerSerializer()
            result = key_serializer(key, SerializationContext(self.topic_name, MessageField.KEY))
            return result
        except Exception as e:
            logger.error(f"Serialization error: {e}")
            raise
        
    
    def value_avro_serialize(self) -> bytes:
        """
        Метод для сериализации avro в байты

        Returns:
            bytes: байты, которые потом можно десериализовать обратно в Avro
        """
        
        try:
            avro_serializer = AvroSerializer(self.schema_registry_client,self.avro_schema)
            value = {"order_id": self.order_id, "subject": self.subject}
            result = avro_serializer(value, SerializationContext(self.topic_name, MessageField.VALUE))
            return result
        except Exception as e:
            logger.error(f"Serialization error: {e}")
            raise
    

    def value_avro_deserialize(self, data: bytes):
        """
        Метод для десериализации байтов обратно в Avro

        Args:
            data (bytes): набор байтов

        Returns:
            dict: объект Avro
        """
        try:
            avro_deserializer = AvroDeserializer(self.schema_registry_client)
            result = avro_deserializer(data, SerializationContext(self.topic_name, MessageField.VALUE))
            return result
        except Exception as e:
            logger.error(f"Serialization error: {e}")
            raise
        
    def key_int_deserialize(self, key: bytes) -> int:
            """
            Метод для десериализации байтов int
            
            Returns:
                int: значение в формате int
            """
            try:
                key_deserializer = IntegerDeserializer()
                result = key_deserializer(key, SerializationContext(self.topic_name, MessageField.KEY))
                return result
            except Exception as e:
                logger.error(f"Serialization error: {e}")
                raise