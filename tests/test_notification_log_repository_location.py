"""Smoke test: NotificationLogRepository lives at
app.services.notifications.repository.notification_log_repository
(the canonical location for the new notification subsystem)."""
from __future__ import annotations

import unittest


class NotificationLogRepositoryLocationTests(unittest.TestCase):
    def test_importable_from_new_subsystem_location(self):
        from app.services.notifications.repository.notification_log_repository import (
            NotificationLogRepository,
        )
        # Class exists and is constructible without args (default db_pool via property).
        self.assertTrue(callable(NotificationLogRepository))

    def test_class_has_record_outcome_method(self):
        from app.services.notifications.repository.notification_log_repository import (
            NotificationLogRepository,
        )
        self.assertTrue(hasattr(NotificationLogRepository, "record_outcome"))


if __name__ == "__main__":
    unittest.main()