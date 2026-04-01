"""
beta_tester_bootstrap.py

First-visit policy genesis for eleutherios.app.

Genesis sequence (Spec §21.4):

  Step 1  P → R → P   child policy created from root axiom
  Step 2  P → M       system creates genesis binding;
                      policy creator is bound as initial manager
  Step 3  M → N       initial manager permissions bound at genesis
  Step 4  P → R → F   defaultForum created via rule (Forum Creation Law)
  Step 5  add_member  manager bound to forum at runtime (not in source)

The policy is not the binding agent. Only the system (at genesis)
or managers holding add_manager (after genesis) create P → M edges.
"""
from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from typing import Optional

from member_permission_registry import MemberPermissionRegistry


GENESIS_MANAGER_PERMISSIONS = [
    "add_manager",
    "add_member",
    "remove_member",
    "can_approve_proposal",
    "can_reject_proposal",
    "can_create_rule",
    "can_delete_rule",
    "view_policy",
    "can_bind_policy",
    "can_register_service",
]

GENESIS_FORUM_MEMBER_PERMISSIONS = [
    "view_policy",
    "can_post_message",
    "can_submit_proposal",
    "can_call_service",
]


@dataclass
class PolicyRecord:
    policy_id:           str
    parent_policy_id:    str
    session_id:          str
    manager_service_id:  str
    manager_permissions: list[str]
    default_forum_id:    str
    created_at:          int


class PolicyRegistry:
    def __init__(self):
        self._records: dict[str, PolicyRecord] = {}

    def store(self, record: PolicyRecord) -> None:
        self._records[record.policy_id] = record

    def by_session(self, session_id: str) -> Optional[PolicyRecord]:
        return next(
            (r for r in self._records.values()
             if r.session_id == session_id),
            None,
        )


@dataclass
class BootstrapResult:
    policy_id:          str
    manager_service_id: str
    default_forum_id:   str
    session_id:         str
    created_at:         int
    genesis_steps:      list[str] = field(default_factory=list)
    is_new_session:     bool = True


class BetaTesterBootstrap:
    """
    Executes the first-visit genesis sequence.
    Idempotent — same session_id always returns the same policy.
    """

    ROOT_POLICY = "betaTestersPolicy"

    def __init__(self, policy_registry: PolicyRegistry,
                 permission_registry: MemberPermissionRegistry):
        self._policies    = policy_registry
        self._permissions = permission_registry

    def bootstrap(self, session_id: str) -> BootstrapResult:
        existing = self._policies.by_session(session_id)
        if existing:
            return BootstrapResult(
                policy_id          = existing.policy_id,
                manager_service_id = existing.manager_service_id,
                default_forum_id   = existing.default_forum_id,
                session_id         = session_id,
                created_at         = existing.created_at,
                genesis_steps      = ["EXISTING_SESSION_RESUMED"],
                is_new_session     = False,
            )

        steps      = []
        policy_id  = f"betaTesterPolicy_{uuid.uuid4().hex[:12]}"
        manager_id = f"defaultUserService_{uuid.uuid4().hex[:8]}"
        forum_id   = f"defaultForum_{uuid.uuid4().hex[:8]}"
        now        = int(time.time())

        # Step 1: P → R → P (restrictive morphology from root)
        steps.append(
            f"STEP_1: P→R→P — {policy_id} created "
            f"extending {self.ROOT_POLICY}"
        )

        # Step 2: Genesis P → M binding
        # System creates this binding. The policy is not the agent.
        # The policy creator is bound as initial manager.
        steps.append(
            f"STEP_2: Genesis P→M — system binds {manager_id} "
            f"to govern {policy_id}"
        )

        # Step 3: M → N — manager permissions bound at genesis
        steps.append(
            f"STEP_3: M→N — permissions bound: {GENESIS_MANAGER_PERMISSIONS}"
        )

        # Step 4: P → R → F — forum created via rule
        # Forum Creation Law: only legal creation path.
        # Forum starts empty.
        steps.append(
            f"STEP_4: P→R→F — {forum_id} created via rule. Forum starts empty."
        )

        # Step 5: Runtime add_member — manager joins their own forum
        # Runtime action only. Never a source declaration.
        self._permissions.add_member(
            forum_id    = forum_id,
            member_id   = manager_id,
            permissions = GENESIS_FORUM_MEMBER_PERMISSIONS,
        )
        steps.append(
            f"STEP_5: add_member — {manager_id} bound to {forum_id}"
        )

        self._policies.store(PolicyRecord(
            policy_id           = policy_id,
            parent_policy_id    = self.ROOT_POLICY,
            session_id          = session_id,
            manager_service_id  = manager_id,
            manager_permissions = GENESIS_MANAGER_PERMISSIONS,
            default_forum_id    = forum_id,
            created_at          = now,
        ))

        return BootstrapResult(
            policy_id          = policy_id,
            manager_service_id = manager_id,
            default_forum_id   = forum_id,
            session_id         = session_id,
            created_at         = now,
            genesis_steps      = steps,
            is_new_session     = True,
        )
