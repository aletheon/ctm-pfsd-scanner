"""
proposal_store.py — Governance proposal data layer.

Stores PENDING/APPROVED/REJECTED proposals submitted via Path B PROPOSAL flow.
In-memory only. No file persistence at this stage.

§48 boundary: stdlib only (uuid, time, dataclasses, typing, enum).
Zone 3 purity: no imports from server.py or compiler.
"""
from __future__ import annotations

import uuid
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class ProposalState(Enum):
    PENDING  = "PENDING"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"


@dataclass
class ProposalRecord:
    proposal_id:      str
    member_id:        str
    forum_id:         str
    policy_id:        str
    content:          str
    state:            ProposalState
    submitted_at:     int
    resolved_at:      Optional[int]   = None
    resolved_by:      Optional[str]   = None
    rejection_reason: Optional[str]   = None
    audit_entry_id:   Optional[str]   = None
    compile_result:   Optional[dict]  = None


class ProposalStore:
    """
    In-memory proposal store.  Stateful — holds records internally.
    Thread safety not required at this stage (single-process).
    """

    def __init__(self) -> None:
        self._records: dict[str, ProposalRecord] = {}

    # ── Public API ─────────────────────────────────────────────────────────

    def submit(
        self,
        member_id:      str,
        forum_id:       str,
        policy_id:      str,
        content:        str,
        audit_entry_id: Optional[str],
    ) -> ProposalRecord:
        """
        Create a new ProposalRecord in PENDING state and store it.

        Raises ValueError if member_id or forum_id is empty.
        """
        if not member_id:
            raise ValueError("member_id must not be empty")
        if not forum_id:
            raise ValueError("forum_id must not be empty")

        proposal_id = "PROP-" + uuid.uuid4().hex[:12]

        record = ProposalRecord(
            proposal_id    = proposal_id,
            member_id      = member_id,
            forum_id       = forum_id,
            policy_id      = policy_id,
            content        = content,
            state          = ProposalState.PENDING,
            submitted_at   = int(time.time()),
            audit_entry_id = audit_entry_id,
        )
        self._records[proposal_id] = record
        return record

    def approve(self, proposal_id: str, manager_id: str) -> ProposalRecord:
        """
        Transition PENDING → APPROVED.

        Raises KeyError if proposal_id not found.
        Raises ValueError if proposal is not PENDING.
        """
        record = self._get_or_raise(proposal_id)
        if record.state is not ProposalState.PENDING:
            raise ValueError(
                f"Proposal {proposal_id} is {record.state.value} — "
                "only PENDING proposals can be approved"
            )
        record.state       = ProposalState.APPROVED
        record.resolved_at = int(time.time())
        record.resolved_by = manager_id
        return record

    def reject(
        self,
        proposal_id: str,
        manager_id:  str,
        reason:      str,
    ) -> ProposalRecord:
        """
        Transition PENDING → REJECTED.

        Raises KeyError if proposal_id not found.
        Raises ValueError if proposal is not PENDING.
        """
        record = self._get_or_raise(proposal_id)
        if record.state is not ProposalState.PENDING:
            raise ValueError(
                f"Proposal {proposal_id} is {record.state.value} — "
                "only PENDING proposals can be rejected"
            )
        record.state            = ProposalState.REJECTED
        record.resolved_at      = int(time.time())
        record.resolved_by      = manager_id
        record.rejection_reason = reason
        return record

    def get(self, proposal_id: str) -> Optional[ProposalRecord]:
        """Return the record, or None if not found."""
        return self._records.get(proposal_id)

    def list_by_forum(
        self,
        forum_id:     str,
        state_filter: Optional[ProposalState] = None,
    ) -> list[ProposalRecord]:
        """
        Return all proposals for a forum, optionally filtered by state.
        Ordered by submitted_at ASC.
        """
        records = [
            r for r in self._records.values()
            if r.forum_id == forum_id
            and (state_filter is None or r.state is state_filter)
        ]
        return sorted(records, key=lambda r: r.submitted_at)

    # ── Internal ───────────────────────────────────────────────────────────

    def _get_or_raise(self, proposal_id: str) -> ProposalRecord:
        record = self._records.get(proposal_id)
        if record is None:
            raise KeyError(f"Proposal '{proposal_id}' not found")
        return record
