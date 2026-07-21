from __future__ import annotations

from typing import Any
from uuid import UUID

from db.postgres_client import get_postgres_client


class MentionsService:
    """Read-side service for the user's @mention inbox."""

    def __init__(self) -> None:
        self.pool = get_postgres_client().get_pool()

    def list_for_user(
        self,
        user_id: str,
        limit: int = 20,
        offset: int = 0,
        status: str | None = None,
        unread_only: bool = False,
    ) -> dict[str, Any]:
        where = ["mn.recipient_user_id = %s"]
        params: list[Any] = [user_id]

        if status:
            where.append("mn.status = %s")
            params.append(status.upper())
        if unread_only:
            where.append("mn.read_at IS NULL")

        where_sql = " AND ".join(where)

        with self.pool.acquire() as conn:
            cur = conn.cursor()
            cur.execute(
                f"SELECT COUNT(*) FROM mention_notifications mn WHERE {where_sql}",
                params,
            )
            total = cur.fetchone()[0]

            cur.execute(
                """
                SELECT COUNT(*) FROM mention_notifications
                WHERE recipient_user_id = %s AND read_at IS NULL
                """,
                (user_id,),
            )
            unread_count = cur.fetchone()[0]

            cur.execute(
                f"""
                SELECT mn.id, mn.status, mn.read_at, mn.created_at, mn.sent_at,
                       cm.mentioned_by, cm.target_label, cm.target_type,
                       cm.expert_id, cm.comment_id, c.text, e.expert_name
                FROM mention_notifications mn
                JOIN comment_mentions cm ON cm.id  = mn.mention_id
                JOIN comments c          ON c.id   = cm.comment_id
                JOIN experts e           ON e.id   = cm.expert_id
                WHERE {where_sql}
                ORDER BY mn.created_at DESC
                LIMIT %s OFFSET %s
                """,
                [*params, limit, offset],
            )
            items = [
                {
                    "id": str(r[0]),
                    "status": r[1],
                    "read_at": r[2].isoformat() if r[2] else None,
                    "created_at": r[3].isoformat() if r[3] else None,
                    "sent_at": r[4].isoformat() if r[4] else None,
                    "mentioned_by": r[5],
                    "target_label": r[6],
                    "target_type": r[7],
                    "expert_id": str(r[8]),
                    "comment_id": str(r[9]),
                    "comment_snippet": (
                        (r[10][:160] + "…") if r[10] and len(r[10]) > 160 else r[10]
                    ),
                    "expert_name": r[11],
                    "comment_url": f"/circulars/{r[8]}/experts/{r[8]}#comment-{r[9]}",
                }
                for r in cur.fetchall()
            ]
        return {
            "items": items,
            "total": total,
            "limit": limit,
            "offset": offset,
            "unread_count": unread_count,
        }

    def unread_count(self, user_id: str) -> int:
        with self.pool.acquire() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT COUNT(*) FROM mention_notifications
                WHERE recipient_user_id = %s AND read_at IS NULL
                """,
                (user_id,),
            )
            return cur.fetchone()[0]

    def mark_read(self, user_id: str, notif_id: UUID) -> dict[str, Any] | None:
        with self.pool.acquire() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE mention_notifications
                SET read_at = NOW()
                WHERE id = %s AND recipient_user_id = %s
                RETURNING id, read_at
                """,
                (str(notif_id), user_id),
            )
            row = cur.fetchone()
            conn.commit()
        if not row:
            return None
        return {"id": str(row[0]), "read_at": row[1].isoformat()}

    def mark_all_read(self, user_id: str) -> int:
        with self.pool.acquire() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE mention_notifications
                SET read_at = NOW()
                WHERE recipient_user_id = %s AND read_at IS NULL
                """,
                (user_id,),
            )
            updated = cur.rowcount
            conn.commit()
        return updated