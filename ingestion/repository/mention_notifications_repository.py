"""
Persist comment_mentions + fan-out mention_notifications.

`persist()` runs inside the caller's transaction (caller owns commit).
`list_pending()` / `mark_sent()` / `mark_failed()` manage their own connections
and are called from the notification worker.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from notifications.mention_resolver import ResolvedMention


@dataclass(slots=True)
class QueuedMention:
    mention_id: UUID
    notification_id: UUID
    recipient_user_id: str
    recipient_email: str
    status: str   # PENDING | SKIPPED


class MentionNotificationsRepository:
    """Persistence layer for comment_mentions + mention_notifications.

    db_pool may be None at construction time — it will be lazy-loaded from
    the singleton postgres client the first time it's needed. This matches
    NotificationLogRepository so the worker can hold an instance with no
    pool wired in.
    """

    def __init__(self, db_pool: Any = None):
        self.logger = logging.getLogger(__name__)
        self._db_pool = db_pool

    @property
    def db_pool(self) -> Any:
        if self._db_pool is None:
            # imported lazily to avoid circular import at module load
            from db.postgres_client import get_postgres_client
            self._db_pool = get_postgres_client().get_pool()
        return self._db_pool

    def persist(
        self,
        cursor,
        comment_id: UUID,
        expert_id: UUID,
        mentioned_by: str,
        resolved: list[ResolvedMention],
    ) -> list[QueuedMention]:
        """Run inside the caller's transaction (caller owns commit)."""
        queued: list[QueuedMention] = []
        for rm in resolved:
            cursor.execute(
                """
                INSERT INTO comment_mentions
                    (comment_id, expert_id, mentioned_by, target_type, target_id, target_label)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (comment_id, target_type, target_id) DO NOTHING
                RETURNING id
                """,
                (str(comment_id), str(expert_id), mentioned_by,
                 rm.target_type, rm.target_id, rm.target_label),
            )
            row = cursor.fetchone()
            if not row:
                continue  # already exists; skip fan-out
            mention_id = row[0]

            # dedupe recipients by user_id (in case same user appears twice)
            seen: set[str] = set()
            for user_id, email in rm.recipients:
                if user_id in seen:
                    continue
                seen.add(user_id)
                if user_id == mentioned_by:
                    status = "SKIPPED"
                    err = "self-mention"
                else:
                    status = "PENDING"
                    err = None

                cursor.execute(
                    """
                    INSERT INTO mention_notifications
                        (mention_id, recipient_user_id, recipient_email, status, error_message)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (mention_id, recipient_user_id) DO NOTHING
                    RETURNING id
                    """,
                    (mention_id, user_id, email, status, err),
                )
                nrow = cursor.fetchone()
                if not nrow:
                    continue
                queued.append(QueuedMention(
                    mention_id=mention_id, notification_id=nrow[0],
                    recipient_user_id=user_id, recipient_email=email, status=status,
                ))
        return queued

    def list_pending(self, limit: int = 100) -> list[dict[str, Any]]:
        with self.db_pool.acquire() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT mn.id, mn.recipient_user_id, mn.recipient_email, mn.mention_id,
                       cm.comment_id, cm.expert_id, cm.mentioned_by, cm.target_label,
                       cm.target_type, c.text, c.created_at, e.expert_name
                FROM mention_notifications mn
                JOIN comment_mentions cm ON cm.id = mn.mention_id
                JOIN comments c          ON c.id  = cm.comment_id
                JOIN experts e           ON e.id  = cm.expert_id
                WHERE mn.status = 'PENDING'
                ORDER BY mn.created_at
                LIMIT %s
                """,
                (limit,),
            )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]

    def mark_sent(self, notif_id: UUID, log_id: UUID) -> None:
        with self.db_pool.acquire() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE mention_notifications
                SET status='SENT', sent_at=NOW(), notification_log_id=%s
                WHERE id=%s
                """,
                (str(log_id), str(notif_id)),
            )
            conn.commit()

    def mark_failed(self, notif_id: UUID, error: str) -> None:
        with self.db_pool.acquire() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE mention_notifications
                SET status='FAILED', error_message=%s
                WHERE id=%s
                """,
                (error, str(notif_id)),
            )
            conn.commit()
