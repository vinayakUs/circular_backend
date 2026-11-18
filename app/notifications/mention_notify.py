"""
Mention fan-out at comment time — enqueue one outbox job per mentioned
recipient. Delivery (render + send) is the outbox worker's job; this module
only snapshots the email into notification_outbox inside the comment's
transaction, so a saved comment and its notifications commit together.
"""
from __future__ import annotations

from typing import Any, Sequence

from config import Config
from ingestion.repository.comment_mentions_repository import MentionRecipient


def on_comment_mentioned(
    cur: Any,
    comment: Any,
    recipients: Sequence[MentionRecipient],
    outbox: Any,
) -> int:
    """Enqueue EMAIL jobs for one comment's mentions.

    Runs on the caller's cursor/transaction (caller owns the commit).
    Returns the number of jobs enqueued.
    """
    if not recipients:
        return 0

    cur.execute(
        """
        SELECT e.expert_name, cir.id, cir.title, cir.full_reference
        FROM experts e
        JOIN circulars cir ON cir.id = e.circular_id
        WHERE e.id = %s
        """,
        (str(comment.expert_id),),
    )
    row = cur.fetchone()
    if not row:
        return 0
    expert_name, circular_uuid, circular_title, circular_full_reference = row

    comment_url = (
        f"{Config.FRONTEND_BASE_URL}/taskview"
        f"?id={circular_uuid}&expertId={comment.expert_id}"
        f"#comment-{comment.id}"
    )
    enqueued = 0
    for r in recipients:
        job = outbox.enqueue(
            cur,
            user_id=r.user_db_id,
            recipient_email=r.email,
            channel="EMAIL",
            template="mention_notification.html",
            params={
                # Subject lives in params — the outbox table has no subject
                # column; the worker reads params["subject"].
                "subject": (
                    f"You were mentioned in a comment by @{comment.author_user_id}"
                ),
                "mentioned_by_user_id": comment.author_user_id,
                "mentioned_by_name": comment.author_name,
                "target_label": r.target_label,
                "comment_text": comment.text,
                "expert_name": expert_name,
                "comment_url": comment_url,
                "comment_created_at": comment.created_at.strftime("%d %b %Y, %H:%M"),
                "circular_title": circular_title,
                "circular_full_reference": circular_full_reference,
            },
            idempotency_key=f"mention:{r.mention_id}:{r.user_id}",
        )
        enqueued += job is not None
    return enqueued
