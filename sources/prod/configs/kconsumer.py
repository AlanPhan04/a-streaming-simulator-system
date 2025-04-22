from kafka import KafkaConsumer

from configs import Constants


class KConsumer:
    def __init__(self,
                 topic,
                 bootstrap_servers=Constants.KAFKA_BOOTSTRAP_SERVERS,
                 key_deserializer=Constants.CONSUMER_KEY_DESERIALIZER,
                 value_deserializer=Constants.CONSUMER_VALUE_DESERIALIZER) -> None:
        self.topic = topic
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            key_deserializer=key_deserializer,
            value_deserializer=value_deserializer,
        )

        print(f"Consumer for topic {self.topic} is ready.")

    def consume(self) -> None:
        try:
            for message in self.consumer:
                key = message.key
                value = message.value

                print(f"[{self.topic}] Key: {key}, Value: {value}\n")
        except Exception as e:
            print(f"Error consuming message: {e}")
        finally:
            self.consumer.close()
