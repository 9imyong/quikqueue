import os
import json
from aiokafka import AIOKafkaProducer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "jobs")


class KafkaProducerSingleton:
    producer: AIOKafkaProducer | None = None

    @classmethod
    async def start(cls):
        # Called only by lifespan before the application accepts requests.
        candidate = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
        try:
            await candidate.start()
        except BaseException:
            await candidate.stop()
            raise
        cls.producer = candidate

    @classmethod
    async def get(cls):
        if cls.producer is None:
            raise RuntimeError("Kafka producer is not ready")
        return cls.producer

    @classmethod
    async def close(cls):
        producer, cls.producer = cls.producer, None
        if producer is not None:
            await producer.stop()


async def send_job(payload: dict):
    producer = await KafkaProducerSingleton.get()
    await producer.send_and_wait(KAFKA_TOPIC, json.dumps(payload).encode("utf-8"))
