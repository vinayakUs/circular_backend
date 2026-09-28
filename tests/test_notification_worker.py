"""Tests for the migrated notification_worker.py.

Exercises:
  - _build_subject (SEBI / SEBI_MASTER / NSE / default prefixes)
  - _send_circular_batch (correct request shape, sent/failed counts, empty-recipient skip)
  - _send_mention_batch (single-recipient tuple, sent/failed counts, empty-row skip)
  - main() exit codes (0 / 1 / 2 / 0-nothing-to-do)
  - worker module exposes exactly one main() definition
"""
from __future__ import annotations

import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch, MagicMock

import services.notification_worker as worker_mod


# ── Subject builder ────────────────────────────────────────────────────


class BuildSubjectTests(unittest.TestCase):
    def test_sebi_source(self):
        self.assertEqual(
            worker_mod._build_subject("SEBI", "SEBI/HO/MRD/2024/1"),
            "SEBI Circular: SEBI/HO/MRD/2024/1",
        )

    def test_sebi_master_source(self):
        self.assertEqual(
            worker_mod._build_subject("SEBI_MASTER", "MC/2024/1"),
            "Sebi Master Circular: MC/2024/1",
        )

    def test_nse_source(self):
        self.assertEqual(
            worker_mod._build_subject("NSE", "NSE/2024/1"),
            "NSE Circular: NSE/2024/1",
        )

    def test_unknown_source_falls_back(self):
        self.assertEqual(
            worker_mod._build_subject("OTHER", "X/2024/1"),
            "Circular: X/2024/1",
        )

    def test_lowercase_source_normalized(self):
        self.assertEqual(
            worker_mod._build_subject("sebi", "X"),
            "SEBI Circular: X",
        )

    def test_none_source_falls_back(self):
        self.assertEqual(
            worker_mod._build_subject(None, "X"),
            "Circular: X",
        )


# ── Circular batch ─────────────────────────────────────────────────────


def _make_circular_record(
    full_reference="SEBI/HO/2024/1",
    title="Test Circular",
    source="SEBI",
    circular_uuid="11111111-1111-1111-1111-111111111111",
):
    return SimpleNamespace(
        id=circular_uuid,
        full_reference=full_reference,
        source=source,
        title=title,
        department="Markets",
        issue_date=date(2024, 9, 28),
        created_at=datetime(2024, 9, 28, 12, 0, 0, tzinfo=timezone.utc),
        pdf_url="https://x/test.pdf",
        applicable_to_nse=True,
        status="FETCHED",
    )


from datetime import date  # noqa: E402


class CircularBatchTests(unittest.TestCase):
    def _setup_mocks(self, circular_records, summary_text=""):
        """Wire up all the DB/S3 deps so the worker can run end-to-end."""
        circular_repo = MagicMock()
        circular_repo.list_recent_fetched_circulars_for_notification.return_value = circular_records
        summary_repo = MagicMock()
        summary_repo.get_summary_text.return_value = summary_text
        log_repo = MagicMock()
        log_repo.get_notified_circular_ids.return_value = set()

        db_pool = MagicMock()
        pool_ctx = MagicMock()
        db_pool.acquire.return_value.__enter__.return_value = pool_ctx
        db_pool.acquire.return_value.__exit__.return_value = False

        return {
            "CircularRepository": circular_repo,
            "SummaryRepository": summary_repo,
            "NotificationLogRepository": log_repo,
            "db_pool": db_pool,
        }

    def _capturing_service(self):
        """Fake NotificationService that records every NotificationRequest."""
        service = MagicMock()
        captured = []

        def capture_send(request):
            captured.append(request)
            from app.services.notifications.models import DeliveryResult
            return DeliveryResult.ok(
                channel=request.channel,
                recipients=request.recipients,
                delivered_at=datetime.now(timezone.utc),
                latency_ms=10,
            )

        service.send.side_effect = capture_send
        service.supported_channels.return_value = ["smtp_email"]
        service.captured = captured
        return service

    def test_builds_correct_notification_request_for_circular(self):
        record = _make_circular_record()
        mocks = self._setup_mocks([record], summary_text="AI summary")
        service = self._capturing_service()
        recipients = ("alice@example.com", "bob@example.com")

        with patch.multiple(
            "services.notification_worker",
            CircularRepository=MagicMock(return_value=mocks["CircularRepository"]),
            SummaryRepository=MagicMock(return_value=mocks["SummaryRepository"]),
            MentionNotificationsRepository=MagicMock(),
            get_postgres_client=MagicMock(return_value=MagicMock(get_pool=MagicMock(return_value=mocks["db_pool"]))),
            get_notification_service=MagicMock(return_value=service),
        ):
            from app.services.notifications.repository.notification_log_repository import (
                NotificationLogRepository,
            )
            with patch.object(NotificationLogRepository, "__init__", lambda self, db_pool=None: None), \
                 patch.object(NotificationLogRepository, "get_notified_circular_ids",
                              MagicMock(return_value=set())):
                sent, failed = worker_mod._send_circular_batch(service, recipients)

        self.assertEqual((sent, failed), (1, 0))
        self.assertEqual(len(service.captured), 1)
        req = service.captured[0]
        self.assertEqual(req.channel, "smtp_email")
        self.assertEqual(req.recipients, ("alice@example.com", "bob@example.com"))
        self.assertEqual(req.template_name, "circular_notification.html")
        self.assertEqual(req.subject, "SEBI Circular: SEBI/HO/2024/1")
        self.assertEqual(req.correlation_id, "circular:11111111-1111-1111-1111-111111111111")
        self.assertEqual(req.variables["full_reference"], "SEBI/HO/2024/1")
        self.assertEqual(req.variables["title"], "Test Circular")
        self.assertIn("summary_html", req.variables)

    def test_returns_zero_zero_when_no_recipients(self):
        service = self._capturing_service()
        sent, failed = worker_mod._send_circular_batch(service, ())
        self.assertEqual((sent, failed), (0, 0))
        self.assertEqual(len(service.captured), 0)

    def test_tracks_sent_and_failed_counts(self):
        from app.services.notifications.models import DeliveryResult

        record = _make_circular_record(full_reference="A/1")
        record2 = _make_circular_record(full_reference="A/2", circular_uuid="22222222-2222-2222-2222-222222222222")

        service = MagicMock()
        calls = {"n": 0}

        def send(request):
            calls["n"] += 1
            # Fail the first, succeed the rest
            if calls["n"] == 1:
                return DeliveryResult.fail(
                    channel=request.channel,
                    recipients=request.recipients,
                    error_code="smtp_timeout",
                    error_detail="boom",
                    delivered_at=datetime.now(timezone.utc),
                )
            return DeliveryResult.ok(
                channel=request.channel,
                recipients=request.recipients,
                delivered_at=datetime.now(timezone.utc),
                latency_ms=10,
            )

        service.send.side_effect = send
        service.supported_channels.return_value = ["smtp_email"]

        mocks = self._setup_mocks([record, record2])

        with patch.multiple(
            "services.notification_worker",
            CircularRepository=MagicMock(return_value=mocks["CircularRepository"]),
            SummaryRepository=MagicMock(return_value=mocks["SummaryRepository"]),
            MentionNotificationsRepository=MagicMock(),
            get_postgres_client=MagicMock(return_value=MagicMock(get_pool=MagicMock(return_value=mocks["db_pool"]))),
            get_notification_service=MagicMock(return_value=service),
        ):
            from app.services.notifications.repository.notification_log_repository import (
                NotificationLogRepository,
            )
            with patch.object(NotificationLogRepository, "__init__", lambda self, db_pool=None: None), \
                 patch.object(NotificationLogRepository, "get_notified_circular_ids",
                              MagicMock(return_value=set())):
                sent, failed = worker_mod._send_circular_batch(
                    service, ("alice@example.com",)
                )

        self.assertEqual((sent, failed), (1, 1))


# ── Mention batch ──────────────────────────────────────────────────────


class MentionBatchTests(unittest.TestCase):
    def _make_service_capture(self, success=True):
        service = MagicMock()
        captured = []

        def capture(request):
            captured.append(request)
            from app.services.notifications.models import DeliveryResult
            if success:
                return DeliveryResult.ok(
                    channel=request.channel,
                    recipients=request.recipients,
                    delivered_at=datetime.now(timezone.utc),
                    latency_ms=10,
                )
            return DeliveryResult.fail(
                channel=request.channel,
                recipients=request.recipients,
                error_code="unknown_channel",
                error_detail="no registered channel",
                delivered_at=datetime.now(timezone.utc),
            )

        service.send.side_effect = capture
        service.supported_channels.return_value = ["splitter_email"]
        service.captured = captured
        return service

    def _make_mention_repo(self, rows):
        repo = MagicMock()
        repo.list_pending.return_value = rows
        return repo

    def _make_row(self, mention_id=1, recipient="bob@example.com"):
        return {
            "id": mention_id,
            "mentioned_by_user_id": "alice",
            "mentioned_by_name": "Alice",
            "target_label": "@bob",
            "text": "Hey @bob, review this",
            "expert_name": "Compliance",
            "circular_uuid": "abc-123",
            "expert_id": 7,
            "comment_id": 99,
            "created_at": datetime(2024, 9, 28, 12, 0, 0, tzinfo=timezone.utc),
            "recipient_email": recipient,
            "circular_title": "Test Circular",
            "circular_full_reference": "SEBI/HO/2024/1",
        }

    def test_builds_correct_request_for_mention(self):
        rows = [self._make_row()]
        repo = self._make_mention_repo(rows)
        service = self._make_service_capture()

        with patch.multiple(
            "services.notification_worker",
            MentionNotificationsRepository=MagicMock(return_value=repo),
            get_postgres_client=MagicMock(return_value=MagicMock(get_pool=MagicMock())),
            get_notification_service=MagicMock(return_value=service),
        ):
            sent, failed = worker_mod._send_mention_batch(service)

        self.assertEqual((sent, failed), (1, 0))
        self.assertEqual(len(service.captured), 1)
        req = service.captured[0]
        self.assertEqual(req.recipients, ("bob@example.com",))
        self.assertEqual(req.template_name, "mention_notification.html")
        self.assertEqual(req.correlation_id, "mention:1")
        self.assertEqual(req.variables["mentioned_by_user_id"], "alice")
        self.assertEqual(req.variables["comment_text"], "Hey @bob, review this")
        self.assertIn("comment_url", req.variables)
        self.assertTrue(req.variables["comment_url"].startswith("http"))

    def test_returns_zero_zero_when_no_pending_mentions(self):
        repo = self._make_mention_repo([])
        service = self._make_service_capture()

        with patch.multiple(
            "services.notification_worker",
            MentionNotificationsRepository=MagicMock(return_value=repo),
            get_postgres_client=MagicMock(return_value=MagicMock(get_pool=MagicMock())),
            get_notification_service=MagicMock(return_value=service),
        ):
            sent, failed = worker_mod._send_mention_batch(service)

        self.assertEqual((sent, failed), (0, 0))
        self.assertEqual(len(service.captured), 0)


# ── main() exit codes ─────────────────────────────────────────────────


class MainExitCodeTests(unittest.TestCase):
    def _run_main(self):
        # argparse reads sys.argv[1:]; isolate it so pytest args don't leak in.
        with patch("sys.argv", ["notification_worker"]):
            return worker_mod.main()

    def test_zero_when_both_batches_succeed(self):
        with patch.multiple(
            "services.notification_worker",
            _send_circular_batch=MagicMock(return_value=(2, 0)),
            _send_mention_batch=MagicMock(return_value=(1, 0)),
            get_notification_service=MagicMock(),
        ):
            code = self._run_main()
        self.assertEqual(code, 0)

    def test_one_when_any_delivery_failed(self):
        with patch.multiple(
            "services.notification_worker",
            _send_circular_batch=MagicMock(return_value=(2, 0)),
            _send_mention_batch=MagicMock(return_value=(0, 1)),
            get_notification_service=MagicMock(),
        ):
            code = self._run_main()
        self.assertEqual(code, 1)

    def test_two_when_unexpected_exception(self):
        with patch.multiple(
            "services.notification_worker",
            _send_circular_batch=MagicMock(side_effect=RuntimeError("db down")),
            _send_mention_batch=MagicMock(),
            get_notification_service=MagicMock(),
        ):
            code = self._run_main()
        self.assertEqual(code, 2)

    def test_zero_when_nothing_to_do(self):
        with patch.multiple(
            "services.notification_worker",
            _send_circular_batch=MagicMock(return_value=(0, 0)),
            _send_mention_batch=MagicMock(return_value=(0, 0)),
            get_notification_service=MagicMock(),
        ):
            code = self._run_main()
        self.assertEqual(code, 0)


# ── Module shape ──────────────────────────────────────────────────────


class ModuleShapeTests(unittest.TestCase):
    def test_only_one_main_function(self):
        import inspect
        members = inspect.getmembers(worker_mod, inspect.isfunction)
        main_funcs = [name for name, fn in members if name == "main"]
        self.assertEqual(len(main_funcs), 1)


if __name__ == "__main__":
    unittest.main()