import json
import time

from confluent_kafka import Producer

from kafka_admin import setup_admin

server = "localhost:9092"

# Splitting the responsibilities of the admin and the producer to be clearer in the code.
setup_admin(server)

# 1. Create a Kafka producer instance (instance A)
producer = Producer({
    "bootstrap.servers": server,
    "client.id": "instanceA",
    "security.protocol": "SASL_PLAINTEXT",
    "sasl.mechanism": "SCRAM-SHA-512",
    "sasl.username": "instanceA",
    "sasl.password": "instanceA"
})

print("PRODUCER CREATED")


# To be sent to Kafka, the data must be in byte form. It is now converted into a JSON string, then into bytes.
# This ensures compatibility with Kafka message processing, which accepts various types of data in byte sequence form.
def prepare_data(d: dict):
    return json.dumps(d).encode('utf-8')


def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.
    :param err: The error that occurred on None on success.
    :param msg: The message that was sent or failed.
    :return:
    """
    if err is not None:
        print("Message delivery failed: {}".format(err))
    else:
        print("Message delivered to {} [{}]".format(msg.topic(), msg.partition()))


# 3. Send the data to the corresponding topics
# 3.1 send in bad topic
producer.produce("private_topic", "test", callback=delivery_report)

# 3.2 send in good topic
data = json.load(open("data.json", "r"))
for entry in data:
    obj_info = {"obj_id": entry["obj_id"], "ra": entry["ra"], "dec": entry["dec"], "sentAt": time.time()}

    for photo in entry["photometry"]:
        topic = f"photometry_stream_{photo['stream_id']}"
        message = {**obj_info, "photometry": photo}
        producer.produce(topic, prepare_data(message), callback=delivery_report)

    for classification in entry["classifications"]:
        topic = f"classification_group_{classification['group_id']}"
        message = {**obj_info, "classifications": classification}
        producer.produce(topic, prepare_data(message), callback=delivery_report)

    producer.flush()
