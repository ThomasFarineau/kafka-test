from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import json
import time

# first, let's start kafka in your terminal. For that, download kafka from https://kafka.apache.org/downloads

# and run the following commands in the kafka folder to start kafka and zookeeper:
# bin/zookeeper-server-start.sh config/zookeeper.properties
# open a new terminal and run:
# bin/kafka-server-start.sh config/server.properties

# keep both terminal windows open, otherwise the kafka server will stop

admin_client = AdminClient(
    {
        "bootstrap.servers": "localhost:9092"
    }
)

print("ADMIN CLIENT CREATED")

# Demo data to be sent to Kafka
data = [
    {
        "obj_id": "ZTF18abfcmux",
        "ra": 0.0,
        "dec": 0.0,
        "photometry": [
            {
                "mjd": 0.0,
                "mag": 0.0,
                "magerr": 0.0,
                "filter": "g",
                "limiting_mag": 0.0,
                "stream_id": 1,
            },
            {
                "mjd": 0.0,
                "mag": 0.0,
                "magerr": 0.0,
                "filter": "r",
                "limiting_mag": 0.0,
                "stream_id": 2,
            }
        ],
        "classifications": [
            {
                "class": "Unknown",
                "class_probability": 0.0,
                "group_id": 1,
            },
            {
                "class": "Unknown but PRIVATE",
                "class_probability": 0.0,
                "group_id": 2,
            },
        ],
    }

]

# As you can see, for a given object, there are multiple photometry and classification entries.
# BUT, not all of those are visible to the same streams or groups (i.e. not visible to the same users).

# PROBLEM TO SOLVE:
# Instance A of the application (which we mimick here, the producer) wants to send data to another instance (called B)
# through Kafka. However, it should only share the data that wthey agreed to share. That is, only the photometry entries
# that are visible to stream_id 1 and classification entries that are visible to group_id 2.

# 1. Create topics per data type (e.g. photometry, classification) and per stream_id and group_id
# 2. Send the data to the corresponding topics, reformatted to include only the desired entry + the obj information.
# 3. Consume the data as if you were instance B. For that, subsribe to the corresponding topics in a seperate script.

# CAUTION: We want the data to be shared securely. This is ignored here for simplicity, but keep in mind that
# authentication and authorization mechanisms should be in place.

# BONUS: If you have time, please consider implementing the authentication and authorization mechanisms.
# that will be crucial in production to ensure that only the right people have access to the right data.

# 1. Create topics per data type (e.g. photometry, classification) and per stream_id and group_id

# create the topic names first
topic_names = []
for entry in data:
    for photo in entry["photometry"]:
        topic_names.append(f"photometry_stream_{photo['stream_id']}")
    for classification in entry["classifications"]:
        topic_names.append(f"classification_group_{classification['group_id']}")

# Remove duplicates
topic_names = list(set(topic_names))

# and then create the topics object that will be used to create the topics
topics = [NewTopic(topic, num_partitions=1, replication_factor=1) for topic in topic_names]

# actually instantiate the topics with kafka
admin_client.create_topics(new_topics=topics, validate_only=False)

print("CREATED TOPICS")

# Create a Kafka producer instance
producer_config = {
    "bootstrap.servers": "localhost:9092",
    "client.id": "kafka-test-producer",
}
producer = Producer(producer_config)

print("PRODUCER CREATED")


def prepare_data(d: dict):
    # data needs to be bytes-like object to be sent to Kafka
    # so, convert the data to a json string and then to bytes
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
for entry in data:
    obj_info = {"obj_id": entry["obj_id"], "ra": entry["ra"], "dec": entry["dec"]}

    for photo in entry["photometry"]:
        topic = f"photometry_stream_{photo['stream_id']}"
        message = {**obj_info, "photometry": photo}
        producer.produce(topic, prepare_data(message), callback=delivery_report)

    for classification in entry["classifications"]:
        topic = f"classification_group_{classification['group_id']}"
        message = {**obj_info, "classifications": classification}
        producer.produce(topic, prepare_data(message), callback=delivery_report)

    producer.flush()

# 4. Consume the data from the corresponding topics. For that, subscribe to the corresponding topics.
# This is done in the consumer.py file.
