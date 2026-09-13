import asyncio
import os
from pathlib import Path
import sys
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

os.environ['SQLALCHEMY_DB_URI'] = 'sqlite://'
ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'services/api'), str(ROOT / 'services/worker')]

import aiohttp
from celery.exceptions import Retry
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from app import main
from app.models import Base
from app.kafka_producer import KafkaProducerSingleton as Producer
from worker_app import tasks


class ApiTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine('sqlite://', connect_args={'check_same_thread': False}, poolclass=StaticPool)
        Base.metadata.create_all(self.engine)
        self.sessions = sessionmaker(bind=self.engine)
        def db():
            with self.sessions() as session:
                yield session
        main.app.dependency_overrides[main.get_db] = db
        self.client = TestClient(main.app)

    def tearDown(self):
        main.app.dependency_overrides.clear()
        self.client.close()
        self.engine.dispose()

    def test_input_boundaries_and_accepted(self):
        with patch.object(main, 'send_job', new_callable=AsyncMock) as send:
            for text in ['', 'x' * 256]:
                self.assertEqual(self.client.post('/submit', json={'text': text}).status_code, 422)
            send.assert_not_called()
            response = self.client.post('/submit', json={'text': '가' * 255})
            self.assertEqual(response.status_code, 202)
            send.assert_awaited_once()
            result = self.client.get('/results/' + str(response.json()['id']))
            self.assertEqual(result.json()['status'], 'QUEUED')

    def test_missing_job(self):
        self.assertEqual(self.client.get('/results/999').status_code, 404)


class ProducerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncTearDown(self):
        Producer.producer = None

    async def test_failure_cleanup_and_retry(self):
        bad = MagicMock(start=AsyncMock(side_effect=RuntimeError('offline')), stop=AsyncMock())
        good = MagicMock(start=AsyncMock(), stop=AsyncMock())
        with patch('app.kafka_producer.AIOKafkaProducer', side_effect=[bad, good]):
            with self.assertRaises(RuntimeError):
                await Producer.start()
            self.assertIsNone(Producer.producer)
            bad.stop.assert_awaited_once()
            await Producer.start()
            self.assertIs(await Producer.get(), good)
            await Producer.close()
            good.stop.assert_awaited_once()
            self.assertIsNone(Producer.producer)

    async def test_not_visible_until_started(self):
        entered, ready = asyncio.Event(), asyncio.Event()
        async def start():
            entered.set()
            await ready.wait()
        candidate = MagicMock(start=start, stop=AsyncMock())
        with patch('app.kafka_producer.AIOKafkaProducer', return_value=candidate):
            task = asyncio.create_task(Producer.start())
            await entered.wait()
            with self.assertRaises(RuntimeError):
                await Producer.get()
            ready.set()
            await task
            self.assertIs(await Producer.get(), candidate)
            await Producer.close()

    async def test_lifespan_shutdown(self):
        with patch.object(main, 'init_db'), patch.object(Producer, 'start', new_callable=AsyncMock) as start, patch.object(Producer, 'close', new_callable=AsyncMock) as close:
            async with main.lifespan(main.app):
                start.assert_awaited_once()
                close.assert_not_awaited()
            close.assert_awaited_once()


class WorkerTests(unittest.TestCase):
    """process_job은 bind=True라 request 컨텍스트를 직접 넣고 run()을 호출한다."""

    def setUp(self):
        self.addCleanup(tasks.process_job.pop_request)

    def run_task(self, payload, retries=0):
        tasks.process_job.push_request(retries=retries)
        return tasks.process_job.run(payload)

    def test_rejects_payload_without_integer_id(self):
        for payload in [{}, {'id': '1'}, {'id': True}]:
            with self.subTest(payload=payload), self.assertRaises(ValueError):
                self.run_task(payload)
                tasks.process_job.pop_request()
            tasks.process_job.push_request(retries=0)

    def test_success_counts_bytes(self):
        row, db = MagicMock(status='QUEUED'), MagicMock()
        db.get.return_value = row
        with patch.object(tasks, 'SessionLocal', return_value=db), \
             patch.object(tasks, 'fetch_external', new_callable=AsyncMock, return_value='한'.encode()):
            result = self.run_task({'id': 1, 'text': 'hello'})
        self.assertEqual(result['status'], 'DONE')
        self.assertEqual(row.status, 'DONE')
        self.assertEqual(row.note, 'Fetched 3 bytes')
        db.commit.assert_called_once()
        db.close.assert_called()

    def test_already_done_skips_external_call(self):
        db = MagicMock()
        db.get.return_value = MagicMock(status='DONE')
        with patch.object(tasks, 'SessionLocal', return_value=db), \
             patch.object(tasks, 'fetch_external', new_callable=AsyncMock) as fetch:
            result = self.run_task({'id': 1, 'text': 'hello'})
        fetch.assert_not_awaited()
        self.assertTrue(result['skipped'])
        db.commit.assert_not_called()

    def test_missing_row_retries_without_inserting(self):
        db = MagicMock()
        db.get.return_value = None
        with patch.object(tasks, 'SessionLocal', return_value=db), \
             patch.object(tasks.process_job, 'retry', side_effect=Retry()) as retry:
            with self.assertRaises(Retry):
                self.run_task({'id': 42, 'text': 'x'})
        db.add.assert_not_called()
        self.assertIsInstance(retry.call_args.kwargs['exc'], tasks.MissingJobRow)

    def test_transient_error_retries_with_backoff(self):
        row, db = MagicMock(status='QUEUED'), MagicMock()
        db.get.return_value = row
        with patch.object(tasks, 'SessionLocal', return_value=db), \
             patch.object(tasks, 'fetch_external', new_callable=AsyncMock, side_effect=asyncio.TimeoutError()), \
             patch.object(tasks.process_job, 'retry', side_effect=Retry()) as retry:
            with self.assertRaises(Retry):
                self.run_task({'id': 1}, retries=1)
        self.assertEqual(retry.call_args.kwargs['countdown'], tasks.RETRY_BACKOFF_SECONDS * 2)
        db.commit.assert_not_called()

    def test_http_error_and_timeout_mark_failed_after_last_retry(self):
        for error in [aiohttp.ClientResponseError(None, (), status=500), asyncio.TimeoutError()]:
            with self.subTest(error=type(error).__name__):
                row, db = MagicMock(status='QUEUED'), MagicMock()
                db.get.return_value = row
                with patch.object(tasks, 'SessionLocal', return_value=db), \
                     patch.object(tasks, 'fetch_external', new_callable=AsyncMock, side_effect=error):
                    result = self.run_task({'id': 1}, retries=tasks.MAX_RETRIES)
                self.assertEqual(result['status'], 'FAILED')
                self.assertEqual(row.status, 'FAILED')
                self.assertEqual(row.note, 'ERROR: ' + type(error).__name__)
                db.commit.assert_called_once()
                db.close.assert_called()
                tasks.process_job.pop_request()
            tasks.process_job.push_request(retries=0)

    def test_unexpected_error_is_recorded_then_raised(self):
        row, db = MagicMock(status='QUEUED'), MagicMock()
        db.get.return_value = row
        with patch.object(tasks, 'SessionLocal', return_value=db), \
             patch.object(tasks, 'fetch_external', new_callable=AsyncMock, side_effect=ZeroDivisionError()):
            with self.assertRaises(ZeroDivisionError):
                self.run_task({'id': 1})
        self.assertEqual(row.status, 'FAILED')
        self.assertEqual(row.note, 'ERROR: ZeroDivisionError')
        db.commit.assert_called_once()


class HttpTests(unittest.IsolatedAsyncioTestCase):
    async def test_status_checked_before_body(self):
        response = MagicMock()
        response.raise_for_status.side_effect = aiohttp.ClientResponseError(None, (), status=500)
        response.read = AsyncMock()
        request = MagicMock()
        request.__aenter__ = AsyncMock(return_value=response)
        request.__aexit__ = AsyncMock(return_value=False)
        session = MagicMock()
        session.get.return_value = request
        context = MagicMock()
        context.__aenter__ = AsyncMock(return_value=session)
        context.__aexit__ = AsyncMock(return_value=False)
        with patch.object(tasks.aiohttp, 'ClientSession', return_value=context) as factory:
            with self.assertRaises(aiohttp.ClientResponseError):
                await tasks.fetch_external('hello')
            self.assertEqual(factory.call_args.kwargs['timeout'].total, 10)
            response.read.assert_not_awaited()
