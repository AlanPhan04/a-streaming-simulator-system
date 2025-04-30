from multiprocessing import Process
from threading import Thread

from configs import KConsumer, Constants


class Consumer:
    @staticmethod
    def main() -> None:
        processes =[
            Thread(target=KConsumer(topic).consume)
            for topic in Constants.KAFKA_TOPICS
        ]

        for process in processes:
            process.start()

        for process in processes:
            process.join()

if __name__ == "__main__":
    Consumer.main()