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

# Beat 스케줄 예시 (1분마다 더미 태스크)
celery_app.conf.beat_schedule = {
    "heartbeat-every-60s": {
        "task": "worker_app.tasks.heartbeat",
        "schedule": 60.0,
    }
}


@worker_process_init.connect
def _create_tables(**_):
    """스키마 준비는 워커 프로세스 기동 시 한 번만. 태스크마다 DDL을 확인하지 않는다."""
    init_db()
    logging.getLogger(__name__).info("schema ready")


from . import tasks  # noqa: E402  <- tasks 모듈 명시적으로 import하여 태스크 등록