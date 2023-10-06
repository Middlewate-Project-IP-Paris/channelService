from kafka import KafkaConsumer
# create Kafka consumer instance
consumer = KafkaConsumer("subsCount", bootstrap_servers='localhost:29092', auto_offset_reset='earliest', enable_auto_commit=False)
# poll for new messages in the topic and print them to the console
for message in consumer:
    print(message.value.decode())