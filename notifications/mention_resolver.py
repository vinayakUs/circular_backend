"""
Resolve parsed mentions → concrete (user_id, email) recipients.

DB-only; uses existing UsersRepository + PropertiesRepository.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import UUID

from ingestion.repository.users_repository import UsersRepository, UserRecord
from ingestion.repository.properties_repository import PropertiesRepository


@dataclass(slots=True)
class ResolvedMention:
    target_type: str                        # 'user' | 'department'
    target_id: str                          # users.user_id OR properties.id
    target_label: str                       # display string for the email
    recipients: list[tuple[str, str]]       # [(user_id, email), ...] — empty = skip


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
                    recipients: list[tuple[str, str]] = []
                else:
                    email = self._resolve_email(user)
                    recipients = [(user.user_id, email)] if email else []
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
                recipients = [(u.user_id, self._resolve_email(u)) for u in users]
                recipients = [r for r in recipients if r[1]]  # drop users without resolvable email
                out.append(ResolvedMention("department", str(dept.id), f"@dep:{dept.name}", recipients))
        return out

    def _find_dept_by_name(self, name: str):
        for d in self.props_repo.list_by_type(self.DEPARTMENT_TYPE, include_archived=False):
            if d.name.lower() == name.lower():
                return d
        return None

    @staticmethod
    def _resolve_email(user: UserRecord) -> str | None:
        """Resolve the destination email for a user mention.

        Priority:
          1. The ``email`` column on the user record (if set) — production truth,
             comes from LDAP / SSO / profile sync.
          2. Synthesised ``<user_id>@<MENTION_EMAIL_DOMAIN>`` — dev fallback.
        """
        if user.email:
            return user.email
        import os
        domain = os.environ.get("MENTION_EMAIL_DOMAIN", "yourcompany.com")
        return f"{user.user_id}@{domain}"
