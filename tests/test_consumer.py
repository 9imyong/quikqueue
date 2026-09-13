import asyncio
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'services/consumer')]

import run_consumer as rc


def message(value, partition=0, offset=7):
    return MagicMock(value=value, partition=partition, offset=offset, topic='jobs')


class ParseTests(unittest.TestCase):
    def test_accepts_payload_with_and_without_type(self):
        for raw in [b'{"id": 1, "text": "hi"}', b'{"id": 1, "text": "hi", "type": "process_job"}']:
            self.assertEqual(rc.parse_message(raw), {'id': 1, 'text': 'hi'})

    def test_defaults_missing_text(self):
        self.assertEqual(rc.parse_message(b'{"id": 3}'), {'id': 3, 'text': ''})

    def test_rejects_bad_messages(self):
        bad = [
            b'not json', b'[]', b'"str"', b'\xff\xfe',
            b'{"id": 1, "type": "other"}',
            b'{"text": "no id"}', b'{"id": "1"}', b'{"id": true}',
            b'{"id": 1, "text": 5}',
        ]
        for raw in bad:
            with self.subTest(raw=raw):
                with self.assertRaises(rc.InvalidMessage):
                    rc.parse_message(raw)


class HandleMessageTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.consumer = MagicMock(commit=AsyncMock())
        self.producer = MagicMock(send_and_wait=AsyncMock())

    async def test_valid_message_dispatched_then_committed(self):
        order = []
        with patch.object(rc, 'dispatch', new_callable=AsyncMock) as dispatch:
            dispatch.side_effect = lambda payload: order.append('dispatch')
            self.consumer.commit.side_effect = lambda: order.append('commit')
            await rc.handle_message(self.consumer, self.producer, message(b'{"id": 9, "text": "x"}'))
        dispatch.assert_awaited_once_with({'id': 9, 'text': 'x'})
        self.producer.send_and_wait.assert_not_awaited()
        self.assertEqual(order, ['dispatch', 'commit'])

    async def test_poison_pill_goes_to_dlq_and_is_committed(self):
        with patch.object(rc, 'dispatch', new_callable=AsyncMock) as dispatch:
            await rc.handle_message(self.consumer, self.producer, message(b'broken'))
        dispatch.assert_not_awaited()
        topic, value = self.producer.send_and_wait.await_args.args
        self.assertEqual(topic, rc.KAFKA_DLQ_TOPIC)
        self.assertEqual(value, b'broken')
        headers = dict(self.producer.send_and_wait.await_args.kwargs['headers'])
        self.assertEqual(headers['source-offset'], b'7')
        self.consumer.commit.assert_awaited_once()

    async def test_dlq_failure_leaves_offset_uncommitted(self):
        self.producer.send_and_wait.side_effect = RuntimeError('kafka down')
        with self.assertRaises(RuntimeError):
            await rc.handle_message(self.consumer, self.producer, message(b'broken'))
        self.consumer.commit.assert_not_awaited()

    async def test_dispatch_retries_then_commits(self):
        attempts = []

        async def flaky(payload):
            attempts.append(payload)
            if len(attempts) < 3:
                raise ConnectionError('redis down')

        with patch.object(rc, 'dispatch', side_effect=flaky), \
             patch.object(rc.asyncio, 'sleep', new_callable=AsyncMock) as sleep:
            await rc.handle_message(self.consumer, self.producer, message(b'{"id": 2}'))
        self.assertEqual(len(attempts), 3)
        self.assertEqual([c.args[0] for c in sleep.await_args_list], [1.0, 2.0])
        self.consumer.commit.assert_awaited_once()

    async def test_dispatch_exhausted_does_not_commit_or_dlq(self):
        with patch.object(rc, 'dispatch', new_callable=AsyncMock, side_effect=ConnectionError('down')), \
             patch.object(rc.asyncio, 'sleep', new_callable=AsyncMock):
            with self.assertRaises(ConnectionError):
                await rc.handle_message(self.consumer, self.producer, message(b'{"id": 2}'))
        self.consumer.commit.assert_not_awaited()
        self.producer.send_and_wait.assert_not_awaited()


class ConsumeLoopTests(unittest.IsolatedAsyncioTestCase):
    async def test_stops_on_signal_without_dropping_batch(self):
        stopping = asyncio.Event()
        msgs = [message(b'{"id": 1}'), message(b'{"id": 2}')]
        consumer = MagicMock(getmany=AsyncMock(return_value={'tp': msgs}))
        handled = []

        async def handle(_c, _p, msg):
            handled.append(msg)
            stopping.set()

        with patch.object(rc, 'handle_message', side_effect=handle):
            await rc.consume(consumer, MagicMock(), stopping)
        self.assertEqual(handled, msgs)
        consumer.getmany.assert_awaited_once()

    async def test_idle_poll_checks_stop_flag(self):
        stopping = asyncio.Event()

        async def getmany(**_):
            stopping.set()
            return {}

        consumer = MagicMock(getmany=getmany)
        await rc.consume(consumer, MagicMock(), stopping)


class ManualCommitConfigTests(unittest.IsolatedAsyncioTestCase):
    async def test_auto_commit_disabled(self):
        created = MagicMock(start=AsyncMock(), stop=AsyncMock())
        with patch.object(rc, 'AIOKafkaConsumer', return_value=created) as factory:
            self.assertIs(await rc.start_consumer_with_retry(), created)
        self.assertFalse(factory.call_args.kwargs['enable_auto_commit'])

    async def test_connection_error_retries_and_closes_candidate(self):
        bad = MagicMock(start=AsyncMock(side_effect=rc.KafkaConnectionError('x')), stop=AsyncMock())
        good = MagicMock(start=AsyncMock(), stop=AsyncMock())
        with patch.object(rc, 'AIOKafkaConsumer', side_effect=[bad, good]), \
             patch.object(rc.asyncio, 'sleep', new_callable=AsyncMock):
            self.assertIs(await rc.start_consumer_with_retry(retries=2, delay=0), good)
        bad.stop.assert_awaited_once()


if __name__ == '__main__':
    unittest.main()
