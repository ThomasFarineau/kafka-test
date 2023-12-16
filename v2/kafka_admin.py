# admin.py
import json

from confluent_kafka.admin import AdminClient, NewTopic, AclBinding, AclOperation, AclPermissionType, ResourceType, \
    ResourcePatternType, ScramMechanism, ScramCredentialInfo, UserScramCredentialsDescription, \
    UserScramCredentialAlteration, UserScramCredentialUpsertion


def generateProducerInstance(name):
    return AclBinding(
        restype=ResourceType.TOPIC,
        name=name,
        resource_pattern_type=ResourcePatternType.LITERAL,
        principal="User:instanceA",
        host="*",
        operation=AclOperation.WRITE,
        permission_type=AclPermissionType.ALLOW
    )


def generateConsumerInstance(instance_name, name):
    return AclBinding(
        restype=ResourceType.TOPIC,
        name=name,
        resource_pattern_type=ResourcePatternType.LITERAL,
        principal=f"User:{instance_name}",
        host="*",
        operation=AclOperation.READ,
        permission_type=AclPermissionType.ALLOW)


def get_topics():
    # Demo data to be sent to Kafka
    data = json.load(open("data.json", "r"))

    # Create the topic names first
    topics = []
    for entry in data:
        for photo in entry["photometry"]:
            topics.append(f"photometry_stream_{photo['stream_id']}")
            for classification in entry["classifications"]:
                topics.append(f"classification_group_{classification['group_id']}")

    # Remove duplicates
    topics = list(set(topics))

    return topics


def get_acl_bindings():
    # Generate ACL bindings
    acl_bindings = []
    for topic_name in get_topics():
        acl_bindings.append(generateProducerInstance(topic_name))
        if topic_name == "photometry_stream_1" or topic_name == "classification_group_2":
            acl_bindings.append(generateConsumerInstance("instanceB", topic_name))
        elif topic_name == "photometry_stream_2" or topic_name == "classification_group_1":
            acl_bindings.append(generateConsumerInstance("instanceC", topic_name))
    return acl_bindings


def setup_admin(server):
    admin_client = AdminClient({
        "bootstrap.servers": server
    })

    print("ADMIN CLIENT CREATED")

    admin_client.create_acls(get_acl_bindings())

    topics = [NewTopic(topic, num_partitions=1, replication_factor=1) for topic in get_topics()]
    # add a new topic "private_topic" which is not accessible by instance A (producer)
    topics.append(NewTopic("private_topic", num_partitions=1, replication_factor=1))

    # Create the topics
    admin_client.create_topics(new_topics=topics, validate_only=False)

    print("CREATED TOPICS")
