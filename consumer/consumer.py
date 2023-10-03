from confluent_kafka import Consumer, KafkaError
import vars

def kafka_consumer_thread(topic):
    consumer = Consumer({
        'bootstrap.servers': vars.KAFKA_BROKER_URL,
        # 'group.id': 'your_consumer_group',
        'auto.offset.reset': 'earliest'
    })

    consumer.subscribe([topic])

    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"Error: {msg.error()}")
                break

        message = msg.value().decode('utf-8')
        # Process the Kafka message (e.g., store it in PostgreSQL)
        print(message)
