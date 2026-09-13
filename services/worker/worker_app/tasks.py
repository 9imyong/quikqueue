import asyncio
import logging
import os

import aiohttp
from celery import shared_task

from .db import SessionLocal
from .models import JobResult

log = logging.getLogger(__name__)

EXTERNAL_API = os.getenv("EXTERNAL_API", "https://httpbin.org/get")

EXTERNAL_API_TIMEOUT_SECONDS = float(os.getenv("EXTERNAL_API_TIMEOUT_SECONDS", "10"))
if EXTERNAL_API_TIMEOUT_SECONDS <= 0:
    raise ValueError("EXTERNAL_API_TIMEOUT_SECONDS must be positive")

MAX_RETRIES = int(os.getenv("TASK_MAX_RETRIES", "3"))
RETRY_BACKOFF_SECONDS = float(os.getenv("TASK_RETRY_BACKOFF_SECONDS", "5"))

# 재시도하면 성공할 수 있는 오류. 그 밖의 예외는 즉시 FAILED로 확정한다.
RETRYABLE_ERRORS = (aiohttp.ClientError, asyncio.TimeoutError, OSError)


class MissingJobRow(Exception):
    """API가 만든 행이 아직 보이지 않는 상태."""


async def fetch_external(text: str):
    timeout = aiohttp.ClientTimeout(total=EXTERNAL_API_TIMEOUT_SECONDS)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(EXTERNAL_API, params={"q": text}) as response:
            response.raise_for_status()
            return await response.read()


def _require_job_id(payload: dict) -> int:
    job_id = payload.get("id")
    if not isinstance(job_id, int) or isinstance(job_id, bool):
        raise ValueError("payload requires an integer id")
    return job_id


def _load_status(job_id: int):
    db = SessionLocal()
    try:
        row = db.get(JobResult, job_id)
        return row.status if row is not None else None
    finally:
        db.close()


def _record(job_id: int, status: str, note: str) -> dict:
    """결과를 기록한다. 행이 없으면 만들어내지 않고 예외로 알린다."""
    db = SessionLocal()
    try:
        row = db.get(JobResult, job_id)
        if row is None:
            raise MissingJobRow(job_id)
        row.status = status
        row.note = note
        db.add(row)
        db.commit()
    finally:
        db.close()
    return {"id": job_id, "status": status}


def _backoff(retries: int) -> float:
    return RETRY_BACKOFF_SECONDS * (2 ** retries)


@shared_task(
    bind=True,
    name="worker_app.tasks.process_job",
    acks_late=True,               # 처리 완료 후 ack: 워커가 죽으면 다른 워커가 다시 받는다
    reject_on_worker_lost=True,
    max_retries=MAX_RETRIES,
)
def process_job(self, payload: dict):
    """
    Kafka consumer → Celery 로 전달된 작업을 처리.
    1) 외부 API를 aiohttp로 호출 (간단한 GET)
    2) 결과를 MySQL job_results 테이블에 반영

    at-least-once 전달이므로 같은 작업이 두 번 올 수 있다. 이미 DONE이면
    외부 호출을 다시 하지 않고 빠져나온다.
    """
    job_id = _require_job_id(payload)
    text = payload.get("text") or ""

    status = _load_status(job_id)
    if status is None:
        # API 커밋이 아직 보이지 않는 경우가 있어 잠시 뒤 다시 본다.
        raise self.retry(exc=MissingJobRow(job_id), countdown=_backoff(self.request.retries))
    if status == "DONE":
        log.info("job %s already done, skipping", job_id)
        return {"id": job_id, "status": "DONE", "skipped": True}

    try:
        content = asyncio.run(fetch_external(text))
    except RETRYABLE_ERRORS as exc:
        if self.request.retries < MAX_RETRIES:
            log.warning("job %s retrying after %s", job_id, type(exc).__name__)
            raise self.retry(exc=exc, countdown=_backoff(self.request.retries))
        # Avoid storing URLs, query text or unbounded exception messages.
        return _record(job_id, "FAILED", f"ERROR: {type(exc).__name__}")
    except Exception as exc:
        # 예상하지 못한 오류도 QUEUED로 방치하지 않고 FAILED로 확정한다.
        log.exception("job %s failed unexpectedly", job_id)
        _record(job_id, "FAILED", f"ERROR: {type(exc).__name__}")
        raise

    return _record(job_id, "DONE", f"Fetched {len(content)} bytes")


@shared_task(name="worker_app.tasks.heartbeat")
def heartbeat():
    # 간단한 로그성 태스크
    return "beat ok"
