

from confluent_kafka import Producer
from models.message import Message
import logging.config
import random
import time
from confluent_kafka.schema_registry import SchemaRegistryClient
from config.schema_registry import SCHEMA_REGISTRY_CONFIG, AVRO_SCHEMA
from settings.logging import LOGGING_CONFIG


logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger("producer_app")

TOPIC_NAME = 'm1_pw2'
SCHEMA_REGISTRY_CONFIG = SCHEMA_REGISTRY_CONFIG
AVRO_SCHEMA = AVRO_SCHEMA

def get_schema_registry_client():
    return SchemaRegistryClient(SCHEMA_REGISTRY_CONFIG)


def delivery_report(err, msg):
    if err:
        logger.error(f"Ошибка при отправке сообщения: {err}")
    else:
        logger.info(
            f"Сообщение успешно отправлено: topic='{msg.topic()}', partition={msg.partition()}, offset={msg.offset()}"
        )
        

def produce_message():
    
    conf = {
        
    "bootstrap.servers": "kafka-0:9092,kafka-1:9092,kafka-2:9092",
    "acks": "all",  # Гарантия «at-least-once»
    "retries": 3,   # Поведение при сбоях
     
    }
    
    # Создание продюсера
    producer = Producer(conf)
    
    try:
        for i in range(1,100):
            
            logger.info(f"Отправка сообщения {i}")
            message = Message(order_id = i+random.randint(1, 100),
                              subject=f"Некий предмет заказа {i}", 
                              topic_name=TOPIC_NAME, 
                              schema_registry_client=SchemaRegistryClient(SCHEMA_REGISTRY_CONFIG),
                              avro_schema=AVRO_SCHEMA
                              )
            
            try:
                producer.produce(
                    topic = TOPIC_NAME,
                    key = message.key_int_serialize(),
                    value = message.value_avro_serialize(),
                    callback=delivery_report,
                )
                producer.poll(0)
                time.sleep(1)
            except Exception as e:
                logger.error(f"Ошибка при отправке сообщения: {e}")
                
    except KeyboardInterrupt:
            logger.info("Получен сигнал остановки.")
    finally:
        producer.flush()
        
if __name__ == "__main__":
    produce_message()