# services/consumer/run_consumer.py
import os, asyncio, json
import logging
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaConnectionError
from celery import Celery

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "jobs")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

celery_app = Celery("bridge")
celery_app.conf.broker_url = REDIS_URL

async def start_consumer_with_retry(retries=30, delay=2.0):
    for i in range(retries):
        try:
            c = AIOKafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id="job-consumers",
                enable_auto_commit=True,
                auto_offset_reset="earliest",
            )
            await c.start()
            return c
        except KafkaConnectionError:
            if i == retries - 1:
                raise
            await asyncio.sleep(delay)

async def main():
    consumer = await start_consumer_with_retry()
    print("[consumer] started")
    try:
        async for msg in consumer:
            payload = json.loads(msg.value.decode("utf-8"))
            if payload.get("type", "process_job") != "process_job":
                logging.error("Unsupported job type at partition=%s offset=%s", msg.partition, msg.offset)
                continue
            task = "worker_app.tasks.process_job"
            celery_app.send_task(task, args=[payload])
    finally:
        await consumer.stop()

if __name__ == "__main__":
    asyncio.run(main())
