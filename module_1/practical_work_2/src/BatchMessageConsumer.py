import logging.config

from confluent_kafka import Consumer, KafkaError, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from config.schema_registry import SCHEMA_REGISTRY_CONFIG

from models.message import Message
from settings.logging import LOGGING_CONFIG

logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger("BatchMessageConsumer_app")


def get_schema_registry_client():
    return SchemaRegistryClient(SCHEMA_REGISTRY_CONFIG)


schema_registry_client = get_schema_registry_client()


def BatchMessageConsumer():
    """
    Считывает минимум по 10 сообщений за один цикл обработки,
    обрабатывает сообщения в цикле и один раз коммитит оффсет после обработки пачки.
    """
    conf = {
        "bootstrap.servers": "kafka-0:9092,kafka-1:9092,kafka-2:9092",
        "group.id": 'consumer_group-2',
        "auto.offset.reset": "earliest",    # читаем с начала топика
        "enable.auto.commit": False,        # отключаем автокоммит
        "enable.auto.offset.store": False,  # оффсеты сохраняем только явным commit()
    }

    consumer = Consumer(conf)
    consumer.subscribe(['m1_pw2'])

    message_helper = Message(
        topic_name='m1_pw2',
        schema_registry_client=schema_registry_client
    )

    BATCH_SIZE = 10
    buffer = []

    try:
        while True:
            # Получаем одно сообщение за вызов poll()
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                # Сообщений нет. Если в буфере что-то накопилось —
                # обрабатываем неполную пачку, чтобы не держать данные в памяти бесконечно.
                if buffer:
                    try:
                        for buffered_msg in buffer:
                            value = message_helper.value_avro_deserialize(buffered_msg.value())
                            logger.info(f"Обработано сообщение: {value}")

                        last_msg = buffer[-1]
                        tp = TopicPartition(
                            last_msg.topic(),
                            last_msg.partition(),
                            last_msg.offset() + 1
                        )
                        consumer.commit(offsets=[tp], asynchronous=False)
                        logger.info(
                            f"Обработана и закоммичена неполная пачка из {len(buffer)} сообщений."
                        )
                    except Exception as e:
                        logger.error(f"Ошибка при обработке неполной пачки: {e}. Коммит не выполнен.")
                    finally:
                        buffer = []
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # Служебное уведомление: дочитали партицию до конца
                    continue
                logger.error(f"Ошибка консьюмера: {msg.error()}")
                continue

            # Добавляем валидное сообщение в буфер
            buffer.append(msg)

            # Как только накопили минимум BATCH_SIZE — обрабатываем пачку
            if len(buffer) >= BATCH_SIZE:
                try:
                    for buffered_msg in buffer:
                        try:
                            value = message_helper.value_avro_deserialize(buffered_msg.value())
                            logger.info(f"Обработано сообщение: {value}")
                        except Exception as e:
                            logger.error(f"Ошибка при обработке сообщения: {e}")
                            raise e

                    last_msg = buffer[-1]
                    tp = TopicPartition(
                        last_msg.topic(),
                        last_msg.partition(),
                        last_msg.offset() + 1
                    )
                    consumer.commit(offsets=[tp], asynchronous=False)
                    logger.info(
                        f"Обработана и закоммичена пачка из {len(buffer)} сообщений."
                    )
                except Exception as e:
                    logger.error(f"Ошибка при обработке пачки: {e}. Коммит не выполнен.")
                finally:
                    buffer = []

    except KeyboardInterrupt:
        logger.info("Получен сигнал остановки.")
    finally:
        consumer.close()


if __name__ == "__main__":
    BatchMessageConsumer()