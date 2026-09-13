import os
import asyncio
import aiohttp
from celery import shared_task
from .db import SessionLocal, init_db
from .models import JobResult

EXTERNAL_API = os.getenv("EXTERNAL_API", "https://httpbin.org/get")

EXTERNAL_API_TIMEOUT_SECONDS = float(os.getenv("EXTERNAL_API_TIMEOUT_SECONDS", "10"))
if EXTERNAL_API_TIMEOUT_SECONDS <= 0:
    raise ValueError("EXTERNAL_API_TIMEOUT_SECONDS must be positive")


async def fetch_external(text: str):
    timeout = aiohttp.ClientTimeout(total=EXTERNAL_API_TIMEOUT_SECONDS)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(EXTERNAL_API, params={"q": text}) as response:
            response.raise_for_status()
            return await response.read()


@shared_task(name="worker_app.tasks.process_job")
def process_job(payload: dict):
    """
    Kafka consumer → Celery 로 전달된 작업을 처리.
    1) 외부 API를 aiohttp로 호출 (간단한 GET)
    2) 결과를 MySQL job_results 테이블에 반영
    """
    init_db()
    job_id = payload["id"]
    text = payload.get("text", "")

    try:
        content = asyncio.run(fetch_external(text))
        note = f"Fetched {len(content)} bytes"
        status = "DONE"
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        # Avoid storing URLs, query text or unbounded exception messages.
        note = f"ERROR: {type(e).__name__}"
        status = "FAILED"

    db = SessionLocal()
    try:
        row = db.get(JobResult, job_id)
        if row:
            row.status = status
            row.note = note
            db.add(row)
            db.commit()
        else:
            # 안전장치: 혹시 API에서 아직 레코드가 없을 경우 생성
            from .models import JobResult as JR
            db.add(JR(id=job_id, input_text=text, status=status, note=note))
            db.commit()
    finally:
        db.close()
    return {"id": job_id, "status": status}

@shared_task(name="worker_app.tasks.heartbeat")
def heartbeat():
    # 간단한 로그성 태스크
    return "beat ok"
