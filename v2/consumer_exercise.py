# We pretend that you are instance B, which has access to stream_id 1 and group_id 2 data.

from confluent_kafka import Consumer

# 1. create a consumer
c = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'auto.offset.reset': 'earliest',
    "client.id": "instanceB",
    "security.protocol": "SASL_PLAINTEXT",
    "sasl.mechanism": "SCRAM-SHA-512",
    "sasl.username": "instanceB",
    "sasl.password": "instanceB"
})
print("CONSUMER CREATED")


# 2. subscribe to the topics that instance B should have access to (refer to the producer exercise)
topics = ["photometry_stream_1", "photometry_stream_2", "classification_group_1", "classification_group_2"]

c.subscribe(topics)
print(f"SUBSCRIBED TO TOPICS: {topics}")

# 3. consume messages from the topics and print them with the topic name
try:
    while True:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue
        print(f"Received message from {msg.topic()}: {msg.value().decode('utf-8')}")
except KeyboardInterrupt:
    pass
finally:
    c.close()