"""
Resolve parsed mentions → concrete (user_id, email) recipients.

DB-only; uses existing UsersRepository + PropertiesRepository.
"""
from __future__ import annotations

from typing import Any
from uuid import UUID

from ingestion.repository.comment_mentions_repository import ResolvedMention
from ingestion.repository.users_repository import UserRecord, UsersRepository
from ingestion.repository.properties_repository import PropertiesRepository


class MentionResolver:
    DEPARTMENT_TYPE = "department"

    def __init__(self, db_pool: Any):
        self.users_repo = UsersRepository(db_pool)
        self.props_repo = PropertiesRepository(db_pool)

    def resolve(self, parsed: list[tuple[str, str]]) -> list[ResolvedMention]:
        out: list[ResolvedMention] = []
        for target_type, target_id in parsed:
            if target_type == "user":
                user = self.users_repo.get_user(target_id)
                if not user:
                    recipients: list[tuple[str, UUID, str]] = []
                else:
                    email = self._resolve_email(user)
                    recipients = [(user.user_id, user.id, email)] if email else []
                out.append(ResolvedMention("user", target_id, f"@{target_id}", recipients))

            elif target_type == "department":
                dept = None
                try:
                    dept = self.props_repo.get_by_id(UUID(target_id))
                except (ValueError, TypeError):
                    dept = self._find_dept_by_name(target_id)
                if not dept:
                    out.append(ResolvedMention("department", target_id, f"@dep:{target_id}", []))
                    continue
                users = self.users_repo.get_users_by_department(dept.id)
                recipients = [(u.user_id, u.id, self._resolve_email(u)) for u in users]
                recipients = [r for r in recipients if r[2]]  # drop users without resolvable email
                out.append(ResolvedMention("department", str(dept.id), f"@dep:{dept.name}", recipients))
        return out

    def _find_dept_by_name(self, name: str):
        for d in self.props_repo.list_by_type(self.DEPARTMENT_TYPE, include_archived=False):
            if d.name.lower() == name.lower():
                return d
        return None

    @staticmethod
    def _resolve_email(user: UserRecord) -> str | None:
        """The ``email`` column on the user record (LDAP / SSO / profile sync);
        None when unset — callers drop those recipients."""
        return user.email
