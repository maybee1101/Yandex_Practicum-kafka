
import logging.config

from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from config.schema_registry import SCHEMA_REGISTRY_CONFIG

from models.message import Message
from settings.logging import LOGGING_CONFIG

logging.config.dictConfig(LOGGING_CONFIG)

logger = logging.getLogger("SingleMessageConsumer_app")


def get_schema_registry_client():
    return SchemaRegistryClient(SCHEMA_REGISTRY_CONFIG)
    
schema_registry_client = get_schema_registry_client()

def SingleMessageConsumer():
    """
    Этот консюьмер считывает по одному сообщению, 
    обрабатывает его и коммитит оффсет автоматически
    
    """
    conf = {
    "bootstrap.servers": "kafka-0:9092,kafka-1:9092,kafka-2:9092",
    "group.id": 'consumer_group-1',
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True,             # Автоматический коммит смещений
    "session.timeout.ms": 6_000,            # Время ожидания активности от консьюмера
    }
    
    consumer = Consumer(conf)
    consumer.subscribe(['m1_pw2'])
    
    # Создаём экземпляр Message для десериализации
 
    message_helper = Message(
        topic_name='m1_pw2',
        schema_registry_client=schema_registry_client
    )
    
    try:
        while True:
            # Получение сообщений
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # Служебное уведомление, что консьюмер дочитал партицию до последнего доступного сообщения на момент запроса
                    continue
                else:
                    logger.error(f"Ошибка консьюмера {msg.error()}")
                    continue
            
            try:
                value = message_helper.value_avro_deserialize(msg.value())
                logger.info(f"Обработано сообщение: {value}")
            except Exception as e:
                logger.error(f"Ошибка при обработке сообщения: {e}")
                
    except KeyboardInterrupt:
            logger.info("Получен сигнал остановки.")

    finally:
        # Закрытие консьюмера
        consumer.close()
    
    
if __name__ == "__main__":
    SingleMessageConsumer()