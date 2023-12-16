# Implementation

## Kafka

To set up Kafka, I used Docker to create a Kafka container and a Zookeeper container. This way, I don't need to install Kafka on my machine, and I can start it very easily whenever I want.

I had to set up a configuration to implement changes in the docker compose, because I enabled ACLs in the kafka server.

```bash
cd kafka && docker compose --file docker-compose-v2.yml up -d 
```

## Admin

As before, I create an admin with AdminClient.

Then I create ACLs for topics and groups using two methods: one for creating ACLs for producers and one for consumers.

I have set it up so that the user instanceA can write to photometry_stream_1, photometry_stream_2, classification_group_1, and classification_group_2. That user instanceB can only read photometry_stream_1 and classification_group_2. And that user instanceC can only read photometry_stream_2 and classification_group_1.

### Create the topic
Same as for V1, this time I've added an additional topic to be sure that my ACLs are working.

## Producer

### Configure the producer

To create a producer, I configure the producer with the following settings:

```python
producer = Producer({
    "bootstrap.servers": server,
    "client.id": "instanceA",
    "security.protocol": "SASL_PLAINTEXT",
    "sasl.mechanism": "SCRAM-SHA-512",
    "sasl.username": "instanceA",
    "sasl.password": "instanceA"
})
```

So this time I've added the concept of a user which corresponds to what I've defined in the ACLs, so from that, I'm supposed to have a producer who can write to the topics that I've defined in the ACLs.

### Prepare data

Same as for V1

### Send data

Same as for V1, but to be sure that my ACLs are working, I've added an additional topic to which I send data that I'm not supposed to have access to.

## Consumer

### Configure the consumer

Just like for the producer, I've configured the consumer with connection parameters. Normally, by setting this up, I have access to the topics that I've defined in the ACLs. So, I'm going to subscribe to the 4 topics ``topics = ["photometry_stream_1", "photometry_stream_2", "classification_group_1", "classification_group_2"]`` and normally during the poll, I should receive errors for what I don't have access to, and the message when I'm supposed to receive it.

```python
c = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'auto.offset.reset': 'earliest',
    "client.id": "instanceB",
    "security.protocol": "SASL_PLAINTEXT",
    "sasl.mechanism": "SCRAM-SHA-512",
    "sasl.username": "instanceB",
    "sasl.password": "instanceB"
})
```

### Subscribe to topics

Same as the V1, but I subscribe to the 4 topics.

### Consume data

Same as the V1.

## Conclusion

It doesn't work, no matter the Kafka configuration I put in place, the ACL system does not work. I don't know if it's a problem with Kafka's configuration or my code. But according to Confluent's documentation, I have correctly set up the ACLs and normally I am following the required configuration.

https://docs.confluent.io/platform/current/kafka/authorization.html
