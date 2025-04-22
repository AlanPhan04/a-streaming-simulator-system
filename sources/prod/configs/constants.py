import json
from dataclasses import asdict
from datetime import datetime


class DatetimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


class Constants:
    # General
    KAFKA_BOOTSTRAP_SERVERS = 'localhost:9094'

    KAFKA_TOPICS = ["air", "earth", "water"]

    # Producer
    PRODUCER_KEY_SERIALIZER = str.encode
    PRODUCER_VALUE_SERIALIZER = lambda value: json.dumps(asdict(value), cls=DatetimeEncoder).encode('utf-8')

    # Consumer
    CONSUMER_KEY_DESERIALIZER = lambda key: key.decode() if key else None
    CONSUMER_VALUE_DESERIALIZER = lambda value: json.loads(value.decode("utf-8"))

    def __setattr__(self, key, value):
        raise TypeError("Cannot modify a constant value")
