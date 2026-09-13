import logging
import os

from celery import Celery
from celery.signals import worker_process_init

from .db import init_db

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
RESULT_DB = os.getenv("CELERY_RESULT_DB_URI")  # db+mysql+pymysql://...

celery_app = Celery("worker_app")
celery_app.conf.update(
    broker_url=REDIS_URL,
    result_backend=RESULT_DB,  # 결과를 MySQL에 저장 (celery 결과 테이블 자동 생성)
    task_routes={
        "worker_app.tasks.*": {"queue": "celery"},
    },
    timezone="Asia/Seoul",
    enable_utc=True,
)

STALE_JOB_SWEEP_SECONDS = float(os.getenv("STALE_JOB_SWEEP_SECONDS", "60"))

celery_app.conf.beat_schedule = {
    # 간단한 생존 확인
    "heartbeat-every-60s": {
        "task": "worker_app.tasks.heartbeat",
        "schedule": 60.0,
    },
    # DB에는 남았지만 큐로 넘어가지 못한 작업 복구
    "requeue-stale-jobs": {
        "task": "worker_app.tasks.requeue_stale_jobs",
        "schedule": STALE_JOB_SWEEP_SECONDS,
    },
}


@worker_process_init.connect
def _create_tables(**_):
    """스키마 준비는 워커 프로세스 기동 시 한 번만. 태스크마다 DDL을 확인하지 않는다."""
    init_db()
    logging.getLogger(__name__).info("schema ready")


from . import tasks  # noqa: E402  <- tasks 모듈 명시적으로 import하여 태스크 등록