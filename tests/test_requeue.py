import os
from datetime import timedelta
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

os.environ.setdefault('SQLALCHEMY_DB_URI', 'sqlite://')
ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'services/worker')]

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from worker_app import tasks
from worker_app.models import Base, JobResult, utcnow


class RequeueStaleJobsTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine('sqlite://', connect_args={'check_same_thread': False}, poolclass=StaticPool)
        Base.metadata.create_all(self.engine)
        self.sessions = sessionmaker(bind=self.engine, future=True)
        patcher = patch.object(tasks, 'SessionLocal', self.sessions)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.addCleanup(self.engine.dispose)

    def add(self, status, age_seconds, text='x'):
        stamp = utcnow() - timedelta(seconds=age_seconds)
        with self.sessions() as db:
            row = JobResult(input_text=text, status=status, note='')
            row.created_at = row.updated_at = stamp
            db.add(row)
            db.commit()
            return row.id

    def test_requeues_only_stale_queued_rows(self):
        stale = self.add('QUEUED', tasks.STALE_JOB_SECONDS + 60, text='stuck')
        self.add('QUEUED', 5)                                  # 아직 처리 중일 수 있음
        self.add('DONE', tasks.STALE_JOB_SECONDS + 60)         # 이미 끝남
        self.add('FAILED', tasks.STALE_JOB_SECONDS + 60)       # 재시도 소진
        with patch.object(tasks.process_job, 'apply_async') as dispatch:
            result = tasks.requeue_stale_jobs()
        self.assertEqual(result, {'requeued': 1})
        dispatch.assert_called_once_with(args=[{'id': stale, 'text': 'stuck'}])

    def test_touching_updated_at_prevents_immediate_redispatch(self):
        self.add('QUEUED', tasks.STALE_JOB_SECONDS + 60)
        with patch.object(tasks.process_job, 'apply_async'):
            self.assertEqual(tasks.requeue_stale_jobs()['requeued'], 1)
        with patch.object(tasks.process_job, 'apply_async') as dispatch:
            self.assertEqual(tasks.requeue_stale_jobs()['requeued'], 0)
        dispatch.assert_not_called()

    def test_status_stays_queued_so_the_row_can_be_swept_again(self):
        job_id = self.add('QUEUED', tasks.STALE_JOB_SECONDS + 60)
        with patch.object(tasks.process_job, 'apply_async'):
            tasks.requeue_stale_jobs()
        with self.sessions() as db:
            self.assertEqual(db.get(JobResult, job_id).status, 'QUEUED')

    def test_batch_is_capped(self):
        for _ in range(5):
            self.add('QUEUED', tasks.STALE_JOB_SECONDS + 60)
        with patch.object(tasks, 'STALE_JOB_BATCH', 2), \
             patch.object(tasks.process_job, 'apply_async') as dispatch:
            self.assertEqual(tasks.requeue_stale_jobs()['requeued'], 2)
        self.assertEqual(dispatch.call_count, 2)

    def test_nothing_to_do_is_quiet(self):
        with patch.object(tasks.process_job, 'apply_async') as dispatch:
            self.assertEqual(tasks.requeue_stale_jobs(), {'requeued': 0})
        dispatch.assert_not_called()


class TimestampTests(unittest.TestCase):
    def test_update_bumps_updated_at(self):
        engine = create_engine('sqlite://', connect_args={'check_same_thread': False}, poolclass=StaticPool)
        self.addCleanup(engine.dispose)
        Base.metadata.create_all(engine)
        sessions = sessionmaker(bind=engine, future=True)
        with sessions() as db:
            row = JobResult(input_text='t', status='QUEUED', note='')
            db.add(row)
            db.commit()
            created, first = row.created_at, row.updated_at
            self.assertIsNotNone(created)
            row.status = 'DONE'
            db.commit()
            self.assertEqual(row.created_at, created)
            self.assertGreaterEqual(row.updated_at, first)


if __name__ == '__main__':
    unittest.main()
