from uuid import UUID

from db.postgres_client import get_postgres_client
from ingestion.repository import CommentsRepository


class CommentsService:

    def __init__(self) -> None:
        db_client = get_postgres_client()
        self.repository = CommentsRepository(db_pool=db_client.get_pool())

    def get_comments(self, expert_id: UUID) -> list[dict]:
        comments = self.repository.get_by_expert_id(expert_id)
        return [
            {
                "id": str(c.id),
                "expert_id": str(c.expert_id),
                "user_id": c.user_id,
                "username": c.username,
                "text": c.text,
                "created_at": c.created_at.isoformat(),
            }
            for c in comments
        ]

    def create_comment(
        self, expert_id: UUID, user_id: str, username: str, text: str
    ) -> dict:
        comment = self.repository.create(expert_id, user_id, username, text)
        return {
            "id": str(comment.id),
            "expert_id": str(comment.expert_id),
            "user_id": comment.user_id,
            "username": comment.username,
            "text": comment.text,
            "created_at": comment.created_at.isoformat(),
        }
