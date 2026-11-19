"""
Persist comment_mentions and build the per-recipient fan-out for a comment.

`persist()` runs inside the caller's transaction (caller owns commit) and
returns one item per deliverable recipient (self-mentions excluded), so the
caller can enqueue notification_outbox jobs in the same transaction.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import UUID

@dataclass(slots=True)
class ResolvedMention:
    target_type: str                                  # 'user' | 'department'
    target_id: str                                    # users.user_id OR properties.id
    target_label: str                                 # display string for the email
    recipients: list[tuple[str, UUID, str]]           # [(user_id, user_db_id, email), ...] — empty = skip


@dataclass(slots=True)
class MentionRecipient:
    mention_id: UUID          # comment_mentions.id
    user_id: str              # LDAP uid of the recipient
    user_db_id: UUID          # users.id of the recipient
    email: str
    target_label: str


class CommentMentionsRepository:
    """comment_mentions inserts only — the caller owns the transaction."""

    def persist(
        self,
        cursor: Any,
        comment_id: UUID,
        expert_id: UUID,
        mentioned_by_user_db_id: UUID,
        resolved: list[ResolvedMention],
    ) -> list[MentionRecipient]:
        out: list[MentionRecipient] = []
        for rm in resolved:
            cursor.execute(
                """
                INSERT INTO comment_mentions
                    (comment_id, expert_id, mentioned_by_user_db_id, target_type, target_id, target_label)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (comment_id, target_type, target_id) DO NOTHING
                RETURNING id
                """,
                (str(comment_id), str(expert_id), str(mentioned_by_user_db_id),
                 rm.target_type, rm.target_id, rm.target_label),
            )
            row = cursor.fetchone()
            if not row:
                continue  # already exists; skip fan-out
            mention_id = row[0]

            seen: set[str] = set()   # same user may appear twice for one target
            for user_id, user_db_id, email in rm.recipients:
                if user_id in seen:
                    continue
                seen.add(user_id)
                if user_db_id == mentioned_by_user_db_id:
                    continue  # self-mention
                out.append(MentionRecipient(
                    mention_id=mention_id, user_id=user_id,
                    user_db_id=user_db_id, email=email,
                    target_label=rm.target_label,
                ))
        return out
