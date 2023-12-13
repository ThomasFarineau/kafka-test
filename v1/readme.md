# Implementation

## Kafka

To set up Kafka, I used Docker to create a Kafka container and a Zookeeper container. This way, I don't need to install Kafka on my machine, and I can start it very easily whenever I want.

```bash
cd kafka && docker compose up -d
```

## Producer

### Create a topic

To create the topics, I based them on the content of the data, making sure that the name of each topic was based on each entry (classification with the group id and photometry with the stream id).

In theory, I should improve this to make it automatic, because currently, my system only works with well-defined names. So, I would need a keyword detector in the data to create the topics automatically. For example:

```json
[
  {
    "name": {
      "keyword": "group_id",
      "data": [
        {
          "group_id": 1
        },
        {
          "group_id": 2
        }
      ]
    },
    ...
  }
]
```

Depending on the keywords, I could create the topics automatically.

### Configure the producer

To create a producer, I configure the producer with the following settings:

```python
producer_config = {
    "bootstrap.servers": "localhost:9092",
    "client.id": "kafka-test-producer",
}
producer = Producer(producer_config)
```

That said, I duplicate the servers part with the AdminClient in the file, so I should create a function (a constant) that allows me to retrieve the servers and use them in both cases.

### Prepare data

To prepare the data, I used the `json` library to read the data and then I used the `json.dumps()` function to convert the data into a string.

### Send data

Like the topic part, when I want to send data to the topics I create, I use the name in the data arbitrarily. So, it's not dynamic at all. If we ever want to extend the application, we will have to modify the code by adding conditions for each topic, which is not what I want.

A possible evolution of my code is to also detect keywords in the data to send the data to the corresponding topics.

## Consumer

### Configure the consumer

I set up a consumer to read the data from the topics, and I assigned it a group.id, although I admit I'm not quite sure what it is for (except for Kafka identification). I set 'auto.offset.reset' to 'earliest', so the consumer can read the data from the beginning.

```python
consumer_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'instance_b_group',
    'auto.offset.reset': 'earliest'
}

c = Consumer(consumer_config)
```

### Subscribe to topics

I literally subscribe to the topic, and this is not good.

I know that I am the instance b and that I am subscribed to topic group_id 2 and stream_id 1. But if I want to add a topic, I have to modify the code, and I don't like that. A planned evolution is to make the consumer detect the topics to which it should subscribe and do so automatically. For example, if I am the instance b, I should subscribe to topics group_id 2 and stream_id 1. Therefore, I need to detect the keywords group_id and stream_id in the data to subscribe to the corresponding topics. To do this, I must retrieve an array of this kind:

```json
[
  {
    "instance_b": {
      "group_id": 2,
      "stream_id": 1
    }
  }
]
```

### Consume data

To consume the data, I used a 'while True' loop, so the consumer can continuously read the data. I implemented a try-except to handle errors (also to be able to stop polling in case of a keyboard interruption), and I used `json.loads()` to convert the data into JSON.


## Conclusion

For now, I have literally done what was noted in the comments. But upon reviewing, I can easily identify areas for improvement.

**On the producer side**, it's evident that topic creation could be greatly improved by automating the detection of the keyword to take from the data. This would allow for easy evolution of the data without having to modify the code.

**For the consumer side**, I've said everything in the subscribe section. It's necessary to manage the groups and topics dynamically. However, I'm not sure if it's possible or secure to detect existing topics. In any case, we can bypass this by specifying which topics exist, but this is less elegant than the detection solution. In this case, I should reverse the topic names. Instead of 'name_keyword_x', I should use 'keyword_x_name' to enable detection of 'keyword_x' and then 'name'.