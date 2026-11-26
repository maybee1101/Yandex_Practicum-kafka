### Описание проекта :

Проект демонстрирует построение конвейера обработки данных на базе Apache Kafka c использованием Docker. 
В рамках проекта реализованы:
  1. Кластер Kafka из трёх брокеров в режиме KRaft
  2. Продюсер и два консьюмера на Python с сериализацией в Avro через Schema Registry
  3. Docker Compose для оркестрации всех компонентов
  4. Kafka UI для мониторинга кластера

### Структура проекта :

    modile_1/
    
      practical_work_2/
      
        - docker-compose.yml           # Оркестрация Kafka-кластера, Schema Registry, Kafka UI
        
        - Dockerfile.python            # Образ для Python-скриптов
        
        - src/                         # Исходный код Python
              - models/
                 - message.py           # Класс Message (Avro-сериализация/десериализация)
              - config/
                 - schema_registry.py   # Конфигурация Schema Registry
              - settings/
                 - logging.py           # Конфигурация логирования
              - producer.py              # Продюсер сообщений
              - single_consumer.py       # Консьюмер с поштучной обработкой и авто-коммитом
              - batch_consumer.py        # Консьюмер с батчевой обработкой и ручным коммитом
      README.md

### Описания классов :


      Message (models/message.py) :
        Класс-обёртка для сериализации и десериализации сообщений. Содержит Avro-схему и методы для преобразования данных в байты и обратно.      
        Ключевые параметры:
          1. Schema_registry_client — клиент для подключения к Schema Registry
          2. topic_name — имя топика (используется в SerializationContext)
          3. avro_schema — строка с Avro-схемой (record с полями order_id и subject)
        Методы:
          1. key_int_serialize() — сериализует order_id (int) в байты для ключа сообщения. Использует MessageField.KEY.
          2. value_avro_serialize() — сериализует словарь {order_id, subject} в Avro-байты для значения. Использует MessageField.VALUE.
          3. key_int_deserialize(key) — восстанавливает int из байтов ключа.
          4. value_avro_deserialize(data) — восстанавливает словарь из Avro-байтов значения. Схема берётся из Schema Registry по ID, записанному в сообщении.

      Producer (producer_app.py) :
         Отправляет 99 сообщений в топик m1_pw2. Ключ сообщения — целое число (order_id), значение — Avro-запись с полями order_id и subject. 
         Для каждого сообщения генерируется случайный order_id и текстовый subject.
         Ключевые параметры:
           1. bootstrap.servers - список брокеров для подключения. Указаны все три (kafka-0:9092,kafka-1:9092,kafka-2:9092), чтобы при недоступности одного продюсер подключился к другому.
           2. acks=all — продюсер ждёт подтверждения от всех синхронных реплик. Это гарантирует, что сообщение не будет потеряно, даже если лидер партиции упадёт сразу после записи. Обеспечивает семантику at-least-once.
           3. retries=3 — количество повторных попыток при временных сбоях
         Параметры класса Message:
           1. order_id — целое число, используется и как ключ, и как поле в значении.
           2. subject — текстовая строка (описание заказа).
           3. topic_name — имя топика (m1_pw2), передаётся в SerializationContext.
           4. schema_registry_client — клиент Schema Registry для регистрации/чтения Avro-схемы.
           5. avro_schema — строка с Avro-схемой.   
         Принцип работы :
           1. Создаётся Producer с конфигурацией.
           2. В цикле for i in range(1, 100) формируется объект Message со случайным order_id.
           3. Вызывается producer.produce() с ключом (сериализованный int) и значением (Avro-байты).
           4. producer.poll(0) обрабатывает колбэки доставки (вызывает delivery_report) без блокировки.
           5. time.sleep(1) — пауза между сообщениями (для наглядности в учебных целях).
           6. producer.flush() принудительно отправляет все накопленные в локальном буфере продюсера сообщения брокеру Kafka

           
      SingleMessageConsumer (SingleMessageConsumer.py):
        Консьюмер, читающий сообщения по одному через poll().
         Ключевые параметры:
           1. enable.auto.commit=True — оффсет коммитится автоматически фоновым потоком
           2. auto.offset.reset=earliest — при отсутствии оффсета читать с начала
           3. session.timeout.ms=6000 — время ожидания активности консьюмера
         Принцип работы: 
             бесконечный цикл poll(timeout=1.0). Если сообщение есть и оно без ошибки — десериализуется и обрабатывается. Коммит оффсета происходит автоматически.

       BatchMessageConsumer (BatchMessageConsumer.py):
         Консьюмер, накапливающий минимум 10 сообщений перед обработкой.
          Ключевые параметры:
            1. enable.auto.commit=False — оффсет коммитится только явно
            2. enable.auto.offset.store=False — Kafka не запоминает оффсеты автоматически
            3. BATCH_SIZE=10 — минимальный размер пачки
          Принцип работы:
            1. Цикл poll() добавляет сообщения в буфер
            2. Когда в буфере >= 10 сообщений — запускается обработка
            3. После успешной обработки всей пачки формируется TopicPartition с offset + 1 последнего сообщения и вызывается commit(offsets=[tp], asynchronous=False)
            4. Если poll() вернул None, а в буфере есть сообщения — обрабатывается неполная пачка (чтобы не держать данные в памяти)


  ### Инструкция по запуску

  1. Скачать репозиторий и перейти в папку /module_1/practical_work_2
  2. Запустить сборку кластера Kafka (вместе с ним в том числе соберется образ для скриптов Python) командой `docker compose up -d`
  3. Создать топик и проверить его командами из файла topic.txt
  4. В терминале внутри папки ./module_1/practical_work_2 выполните команду для запуска продюсера. Так как по заданию приложение должно работать в двух экземплярах, то запускаем две команды - так мы запустим два контейнера с одним продюсером:

     `docker exec -it practical_work_2-python-scripts-1 python producer_app.py`
     
     `docker exec -it practical_work_2-python-scripts-2 python producer_app.py`
     
  6. В отдельных терминалах также внутри папки ./module_1/practical_work_2 выполните команды для запуска консьюмеров. Каждый запускается в двух экземплярах:

     `docker exec -it practical_work_2-python-scripts-1 python BatchMessageConsumer.py`
     
     `docker exec -it practical_work_2-python-scripts-2 python BatchMessageConsumer.py`


     `docker exec -it practical_work_2-python-scripts-1 python SingleMessageConsumer.py`
     
     `docker exec -it practical_work_2-python-scripts-2 python SingleMessageConsumer.py`

### Шаги проверки решения 

   1. Проверка, что кластер Kafka работает командой 
       docker exec -it kafka-0 kafka-topics.sh \
        --describe \
        --topic m1_pw2 \
        --bootstrap-server kafka-0:9092
      Ожидаемый результат - PartitionCount: 3, ReplicationFactor: 2, три партиции с лидерами на разных брокерах.

   2. Топик создан с правильными параметрами - в выводе describe должно быть указано 2 реплики. Вывод примерно такой :
      
      `Topic: m1_pw2   TopicId: HI5bB6XnRqGKZ3PC2VHt-Q PartitionCount: 3       ReplicationFactor: 2    Configs: 
        Topic: m1_pw2   Partition: 0    Leader: 2       Replicas: 2,0   Isr: 2,0
        Topic: m1_pw2   Partition: 1    Leader: 0       Replicas: 0,1   Isr: 0,1
        Topic: m1_pw2   Partition: 2    Leader: 1       Replicas: 1,2   Isr: 1,2`

    3. Продюсер отправляет сообщения -  в консоли отображаются сообщения об отправке сообщений
    4. Консьюмер принимает сообщения - в консоли отображаются десериализованные сообщения в читаемом виде и уведомления об их обработке
    5. По адресу http://localhost:8081/subjects отображается ["m1_pw2-value"] - зарегистрированная схема Avro
    6. По адресу http://localhost:8080  отображаются топики, партиции, сообщения, консьюмер-группы
      
        
     
  

  
           
        
      

    
