from __future__ import annotations

import logging
from uuid import UUID

from db.postgres_client import get_postgres_client
from ingestion.repository.comments_repository import CommentsRepository
from ingestion.repository.mention_notifications_repository import MentionNotificationsRepository
from notifications.mention_parser import parse_mentions
from notifications.mention_resolver import MentionResolver


logger = logging.getLogger(__name__)


class CommentsService:

    def __init__(self) -> None:
        db_client = get_postgres_client()
        pool = db_client.get_pool()
        self.repository = CommentsRepository(db_pool=pool)
        self.mention_repo = MentionNotificationsRepository(db_pool=pool)
        self.resolver = MentionResolver(db_pool=pool)

    def get_comments(self, expert_id: UUID) -> list[dict]:
        comments = self.repository.get_by_expert_id(expert_id)
        return [
            {
                "id": str(c.id),
                "expert_id": str(c.expert_id),
                "user_db_id": str(c.user_db_id),
                "author_user_id": c.author_user_id,
                "author_name": c.author_name,
                "author_email": c.author_email,
                "text": c.text,
                "created_at": c.created_at.isoformat(),
            }
            for c in comments
        ]

    def create_comment(
        self, expert_id: UUID, user_db_id: UUID, text: str
    ) -> dict:
        """Insert comment + fan out @mentions in a single transaction.

        If mention parsing or resolution raises, the comment is still saved
        (best-effort notification fan-out — never block the user from commenting).
        """
        with self.repository.db_pool.acquire() as conn:
            cur = conn.cursor()
            comment = self.repository._insert_in_tx(cur, expert_id, user_db_id, text)
            try:
                parsed = parse_mentions(text)
                if parsed:
                    resolved = self.resolver.resolve(parsed)
                    self.mention_repo.persist(
                        cur, comment.id, expert_id, comment.user_db_id, resolved
                    )
            except Exception:
                logger.exception(
                    "mention resolution failed; comment %s saved without notifications",
                    comment.id,
                )
            conn.commit()

        return {
            "id": str(comment.id),
            "expert_id": str(comment.expert_id),
            "user_db_id": str(comment.user_db_id),
            "author_user_id": comment.author_user_id,
            "author_name": comment.author_name,
            "author_email": comment.author_email,
            "text": comment.text,
            "created_at": comment.created_at.isoformat(),
        }