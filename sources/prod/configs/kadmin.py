from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError

from configs import Constants


class KAdmin:
    def __init__(self,
                 bootstrap_servers=Constants.KAFKA_BOOTSTRAP_SERVERS) -> None:
        self.admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)

    def setup(self) -> None:
        topics = [
            NewTopic(
                name=topic,
                num_partitions=3,
                replication_factor=1
            )
            for topic in Constants.KAFKA_TOPICS
        ]

        try:
            self.admin.create_topics(new_topics=topics, validate_only=False)
            print("All needed topics created!!!")
        except TopicAlreadyExistsError as e:
            print("All needed topics have already been created!!!")

    def close(self) -> None:
        self.admin.close()
