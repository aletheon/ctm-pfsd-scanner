"""
member_permission_registry.py

Data store for forum membership and permission sets.
Read by the middleware tier during Path B validation.

The forum node (F) does not validate permissions.
The middleware reads this registry and validates.
"""
from __future__ import annotations
from typing import Optional


class MemberPermissionRegistry:
    """
    Stores which services are members of which forums and what
    permissions they hold. Only the middleware reads this for
    Path B validation. The forum itself never reads this store.
    """

    def __init__(self):
        # forum_id → { member_id → set(permissions) }
        self._store: dict[str, dict[str, set[str]]] = {}

    def add_member(self, forum_id: str, member_id: str,
                   permissions: list[str]) -> None:
        """
        Record a manager's runtime add_member action.
        Called by a manager holding add_member permission — never in source.
        """
        self._store.setdefault(forum_id, {})[member_id] = set(permissions)

    def remove_member(self, forum_id: str, member_id: str) -> None:
        """Record a manager's remove_member action."""
        self._store.get(forum_id, {}).pop(member_id, None)

    def has_permission(self, forum_id: str, member_id: str,
                       permission: str) -> bool:
        """
        Returns True if the member holds the required permission.
        Called by the middleware — never by the forum.
        """
        return permission in self._store.get(forum_id, {}).get(member_id, set())

    def is_member(self, forum_id: str, member_id: str) -> bool:
        return member_id in self._store.get(forum_id, {})

    def get_permissions(self, forum_id: str,
                        member_id: str) -> frozenset[str]:
        return frozenset(self._store.get(forum_id, {}).get(member_id, set()))
