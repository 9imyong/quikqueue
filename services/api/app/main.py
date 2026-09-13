from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, HTTPException
from starlette.concurrency import run_in_threadpool
from pydantic import BaseModel, Field
from .db import init_db, SessionLocal
from sqlalchemy.orm import Session
from .models import JobResult
from .kafka_producer import send_job, KafkaProducerSingleton

@asynccontextmanager
async def lifespan(app: FastAPI):
    await run_in_threadpool(init_db)
    try:
        await KafkaProducerSingleton.start()
        yield
    finally:
        await KafkaProducerSingleton.close()

app = FastAPI(title="FastAPI + Kafka + Celery + MySQL", lifespan=lifespan)

class JobIn(BaseModel):
    text: str = Field(min_length=1, max_length=255)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.get("/")
def health():
    return {"ok": True}

@app.post("/submit", status_code=202)
async def submit_job(job: JobIn, db: Session = Depends(get_db)):
    # DB에 PENDING 레코드 생성
    row = JobResult(input_text=job.text, status="QUEUED")
    db.add(row)
    db.commit()
    db.refresh(row)

    # Kafka 로 메시지 발행
    await send_job({"id": row.id, "text": job.text})
    return {"id": row.id, "status": "QUEUED"}

@app.get("/results/{job_id}")
def get_result(job_id: int, db: Session = Depends(get_db)):
    row = db.get(JobResult, job_id)
    if not row:
        raise HTTPException(status_code=404, detail="Job not found")
    return {"id": row.id, "status": row.status, "note": row.note, "input_text": row.input_text}
