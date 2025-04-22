from kafka import KafkaProducer

from configs import Constants


class KProducer:
    def __init__(self,
                 bootstrap_servers=Constants.KAFKA_BOOTSTRAP_SERVERS,
                 key_serializer=Constants.PRODUCER_KEY_SERIALIZER,
                 value_serializer=Constants.PRODUCER_VALUE_SERIALIZER) -> None:
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            key_serializer=key_serializer,
            value_serializer=value_serializer,
        )

        print(f"Producer is ready")

    def send(self, topic, data_object):
        try:
            future = self.producer.send(topic, key=data_object.station, value=data_object)
            print(f"Sent {future.get(timeout=10)}\n")
        except Exception as e:
            print(f"Error sending to Kafka topic {topic}: {e}")

    def close(self):
        if self.producer:
            self.producer.close()

