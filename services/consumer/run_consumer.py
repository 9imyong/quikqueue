# services/consumer/run_consumer.py
"""Kafka `jobs` 토픽을 구독해 Celery 태스크로 전달하는 브리지.

전달 보장 방침:
- 오프셋은 자동 커밋하지 않는다. Celery 전달이 성공한 뒤에만 커밋하므로
  최소 1회(at-least-once) 전달이 보장된다. 중복은 워커 쪽에서 방어한다.
- 해석 불가능한 메시지(poison pill)는 DLQ 토픽으로 보내고 커밋해서 건너뛴다.
- 브로커 장애처럼 일시적인 실패는 커밋하지 않고 프로세스를 종료한다.
  컨테이너가 재시작하면 커밋되지 않은 오프셋부터 다시 읽는다.
"""
import asyncio
import json
import logging
import os
import signal

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError
from celery import Celery

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "jobs")
KAFKA_DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "jobs.dlq")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
CONSUMER_GROUP = os.getenv("KAFKA_GROUP_ID", "job-consumers")

DISPATCH_RETRIES = int(os.getenv("CONSUMER_DISPATCH_RETRIES", "5"))
DISPATCH_BACKOFF_SECONDS = float(os.getenv("CONSUMER_DISPATCH_BACKOFF_SECONDS", "1.0"))

SUPPORTED_JOB_TYPE = "process_job"
PROCESS_JOB_TASK = "worker_app.tasks.process_job"

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s [consumer] %(message)s",
)
log = logging.getLogger("consumer")

celery_app = Celery("bridge")
celery_app.conf.broker_url = REDIS_URL


class InvalidMessage(Exception):
    """재시도해도 달라지지 않는 메시지. DLQ로 보낸다."""


def parse_message(raw: bytes) -> dict:
    """Kafka 메시지 본문을 검증된 페이로드로 변환한다."""
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise InvalidMessage(f"decode failed: {type(exc).__name__}") from exc

    if not isinstance(payload, dict):
        raise InvalidMessage("payload is not an object")

    job_type = payload.get("type", SUPPORTED_JOB_TYPE)
    if job_type != SUPPORTED_JOB_TYPE:
        raise InvalidMessage("unsupported job type")

    job_id = payload.get("id")
    # bool은 int의 하위 타입이라 명시적으로 배제한다.
    if not isinstance(job_id, int) or isinstance(job_id, bool):
        raise InvalidMessage("missing or non-integer id")

    text = payload.get("text", "")
    if not isinstance(text, str):
        raise InvalidMessage("text is not a string")

    return {"id": job_id, "text": text}


async def start_consumer_with_retry(retries=30, delay=2.0):
    for attempt in range(retries):
        consumer = AIOKafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id=CONSUMER_GROUP,
            enable_auto_commit=False,  # 전달 성공 후 수동 커밋
            auto_offset_reset="earliest",
        )
        try:
            await consumer.start()
            return consumer
        except KafkaConnectionError:
            await consumer.stop()
            if attempt == retries - 1:
                raise
            await asyncio.sleep(delay)


async def start_producer_with_retry(retries=30, delay=2.0):
    for attempt in range(retries):
        producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
        try:
            await producer.start()
            return producer
        except KafkaConnectionError:
            await producer.stop()
            if attempt == retries - 1:
                raise
            await asyncio.sleep(delay)


async def send_to_dlq(producer, msg, reason: str):
    """원본 바이트를 그대로 보존하고 사유는 헤더에 담는다."""
    headers = [
        ("reason", reason.encode("utf-8")),
        ("source-topic", msg.topic.encode("utf-8")),
        ("source-partition", str(msg.partition).encode("utf-8")),
        ("source-offset", str(msg.offset).encode("utf-8")),
    ]
    await producer.send_and_wait(KAFKA_DLQ_TOPIC, msg.value, headers=headers)


async def dispatch(payload: dict):
    """Celery로 태스크를 보낸다. send_task는 블로킹이라 스레드로 넘긴다."""
    await asyncio.to_thread(celery_app.send_task, PROCESS_JOB_TASK, args=[payload])


async def dispatch_with_retry(payload: dict) -> None:
    """일시적 브로커 장애를 흡수한다. 끝내 실패하면 예외를 올려 프로세스를 내린다."""
    for attempt in range(DISPATCH_RETRIES):
        try:
            await dispatch(payload)
            return
        except Exception as exc:
            if attempt == DISPATCH_RETRIES - 1:
                raise
            log.warning(
                "dispatch failed for job=%s (attempt %s/%s): %s",
                payload["id"], attempt + 1, DISPATCH_RETRIES, type(exc).__name__,
            )
            await asyncio.sleep(DISPATCH_BACKOFF_SECONDS * (2 ** attempt))


async def handle_message(consumer, producer, msg) -> None:
    try:
        payload = parse_message(msg.value)
    except InvalidMessage as exc:
        log.error(
            "dropping message partition=%s offset=%s: %s", msg.partition, msg.offset, exc
        )
        # DLQ 적재에 실패하면 커밋하지 않는다. 메시지를 조용히 잃지 않기 위함.
        await send_to_dlq(producer, msg, str(exc))
        await consumer.commit()
        return

    await dispatch_with_retry(payload)
    await consumer.commit()
    log.info("dispatched job=%s offset=%s", payload["id"], msg.offset)


async def consume(consumer, producer, stopping: asyncio.Event) -> None:
    """종료 신호를 1초 단위로 확인하면서 한 건씩 처리한다."""
    while not stopping.is_set():
        batch = await consumer.getmany(timeout_ms=1000, max_records=1)
        for messages in batch.values():
            for msg in messages:
                await handle_message(consumer, producer, msg)


async def main():
    stopping = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stopping.set)

    consumer = await start_consumer_with_retry()
    producer = await start_producer_with_retry()
    log.info("started: topic=%s group=%s dlq=%s", KAFKA_TOPIC, CONSUMER_GROUP, KAFKA_DLQ_TOPIC)
    try:
        await consume(consumer, producer, stopping)
    finally:
        await consumer.stop()
        await producer.stop()
        log.info("stopped")


if __name__ == "__main__":
    asyncio.run(main())
