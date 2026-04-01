"""
message_validation_middleware.py

Path B permission validation middleware.

  @command flow:    B → G → middleware → S
  Forum post flow:  B → G → middleware → F

Permission validation occurs here.
The forum receives validated messages only.
The forum never validates permissions.
The forum never invokes services.

All audit entries use member_id as the primary identifier.
"""
from __future__ import annotations

import hashlib
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from member_permission_registry import MemberPermissionRegistry


class MessageFlow(Enum):
    COMMAND  = "command"    # @service  →  middleware → S
    POST     = "post"       # message   →  middleware → F
    PROPOSAL = "proposal"   # proposal  →  middleware → F → M → P


_REQUIRED_PERMISSION: dict[MessageFlow, str] = {
    MessageFlow.COMMAND:  "can_call_service",
    MessageFlow.POST:     "can_post_message",
    MessageFlow.PROPOSAL: "can_submit_proposal",
}


@dataclass
class GovernedMessage:
    """
    A governed message produced by a member (B → G).
    member_id is always the authoritative identity.
    """
    message_id:     str
    member_id:      str       # B — authoritative, always
    forum_id:       str       # permission context only
    content:        str
    flow:           MessageFlow
    command_target: Optional[str] = None   # service name for COMMAND flow


@dataclass
class ValidationResult:
    permitted:          bool
    member_id:          str   # always B
    forum_id:           str
    permission_checked: str
    flow:               MessageFlow
    routed_to:          Optional[str] = None  # service_id or forum_id
    error_code:         Optional[str] = None  # RT-E005 if not permitted
    audit_entry_id:     Optional[str] = None


def parse_message(content: str, member_id: str,
                  forum_id: str) -> GovernedMessage:
    """
    Parse raw content into a GovernedMessage.
    Attribution is always to member_id.

    @ServiceName ...  →  COMMAND flow (routes to service)
    propose: ...      →  PROPOSAL flow (routes to forum → M → P)
    anything else     →  POST flow (routes to forum)
    """
    s   = content.strip()
    mid = str(uuid.uuid4())
    if s.startswith("@"):
        parts  = s[1:].split(maxsplit=1)
        target = parts[0] if parts else None
        return GovernedMessage(mid, member_id, forum_id, content,
                               MessageFlow.COMMAND, target)
    if s.lower().startswith(("propose:", "proposal:")):
        return GovernedMessage(mid, member_id, forum_id, content,
                               MessageFlow.PROPOSAL)
    return GovernedMessage(mid, member_id, forum_id, content,
                           MessageFlow.POST)


class MessageValidationMiddleware:
    """
    Path B middleware.

    Routing:
      COMMAND  → service  (B → G → middleware → S)
      POST     → forum    (B → G → middleware → F)
      PROPOSAL → forum    (B → G → middleware → F → M → P)

    The forum_id in the message is the permission context.
    The forum node itself has no role in this validation.
    """

    def __init__(self, registry: MemberPermissionRegistry):
        self._registry  = registry
        self._log: list[dict] = []

    def validate(self, message: GovernedMessage) -> ValidationResult:
        required = _REQUIRED_PERMISSION[message.flow]

        if not self._registry.is_member(message.forum_id, message.member_id):
            return self._deny(message, required)

        if not self._registry.has_permission(
                message.forum_id, message.member_id, required):
            return self._deny(message, required)

        # Route: COMMAND → service, everything else → forum
        routed_to = (message.command_target
                     if message.flow == MessageFlow.COMMAND
                     else message.forum_id)

        aid = self._record(message, required, routed_to, True)
        return ValidationResult(
            permitted          = True,
            member_id          = message.member_id,  # always B
            forum_id           = message.forum_id,
            permission_checked = required,
            flow               = message.flow,
            routed_to          = routed_to,
            audit_entry_id     = aid,
        )

    def _deny(self, message: GovernedMessage,
              required: str) -> ValidationResult:
        aid = self._record(message, required, None, False)
        return ValidationResult(
            permitted          = False,
            member_id          = message.member_id,  # always B
            forum_id           = message.forum_id,
            permission_checked = required,
            flow               = message.flow,
            error_code         = "RT-E005",
            audit_entry_id     = aid,
        )

    def _record(self, message: GovernedMessage, permission: str,
                routed_to: Optional[str], permitted: bool) -> str:
        entry_id = "AUD-" + hashlib.sha256(
            f"{message.message_id}{message.member_id}{time.time()}".encode()
        ).hexdigest()[:12]
        self._log.append({
            "audit_entry_id":     entry_id,
            "attributed_to":      message.member_id,  # B — always
            "forum_id":           message.forum_id,
            "permission_checked": permission,
            "flow":               message.flow.value,
            "routed_to":          routed_to,
            "permitted":          permitted,
        })
        return entry_id

    def audit_log(self) -> list[dict]:
        return list(self._log)
