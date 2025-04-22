import csv
import time
from multiprocessing import Process
from typing import Type, Union

from configs import KProducer, KAdmin
from models import Air, Earth, Water


class Producer:
    @staticmethod
    def setup_broker() -> None:
        kadmin = KAdmin()
        kadmin.setup()
        kadmin.close()

    @staticmethod
    def process_csv(file_name: str, data_class: Type[Union[Air, Earth, Water]], topic: str) -> None:
        producer = KProducer()

        with open(file_name) as csv_file:
            reader = csv.DictReader(csv_file)
            next(reader)
            for row in reader:
                data_object = data_class.from_dict(row)
                producer.send(topic, data_object)

        producer.close()

    @staticmethod
    def main() -> None:
        # Setup broker
        Producer.setup_broker()

        # Define your data sources as tuples (file_path, data_class, topic)
        data_sources = [
            ("../data/AIR2308.csv", Air, "air"),
            ("../data/EARTH2308.csv", Earth, "earth"),
            ("../data/WATER2308.csv", Water, "water")
        ]

        # Create processes using list comprehension
        processes = [
            Process(
                target=Producer.process_csv,
                args=data_source
            )
            for data_source in data_sources
        ]

        # Start all processes
        for process in processes:
            process.start()

        # Wait for all processes to complete
        for process in processes:
            process.join()


if __name__ == '__main__':
    Producer.main()
