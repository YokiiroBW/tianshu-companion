"""NoneBot-side command fast path.

One rule decides everything here: a text is claimed as a direct command only when it is a
registered, complete command for this platform and audience. Everything else keeps following
the existing companion chain.

The module deliberately adds no second message exit and no second de-duplication store:

* the ownership decision and its durable record go through `Bridge.capture`, which already
  authenticates the source, consumes only normalized SDK events and persists one owner per
  message version;
* the outbound reply goes through `Bridge.send`, which already owns destination
  verification, the per-conversation serial position, `reply_id` de-duplication and the
  `unknown` receipt discipline;
* when Core - the authority on what is registered - answers that a claimed text is not a
  registered command here, `Bridge.hand_back` returns the row to the companion queue instead
  of answering it twice.

The matcher is a coarse snapshot. It can only ever mis-route; it can never cause two replies,
because Core re-matches the whole command and its parameter shape and is the only component
that executes anything.
"""

from tianshu_companion.contracts import Fault
from tianshu_companion.direct import configured_matcher

DIRECT_ROUTE = "/internal/v1/conversation/direct-command"
OWNERS = {"direct", "companion"}


def registry_matcher(commands):
    """Bridge matcher built from the registered command snapshot (name + platform scopes).

    Only a `reply_to="bridge"` registration is claimable here. A registration whose reply
    owner is Core is capability-only, so its bare text must keep following the companion
    chain: a direct command must never start depending on the chat model being reachable.
    """
    names = {}
    for command in commands:
        if command["reply_to"] != "bridge":
            continue
        for namespace in command["platforms"]:
            for audience in command["audiences"]:
                names.setdefault((namespace, audience), set()).update(command["names"])
    return configured_matcher(names)


class DirectRouter:
    """Registered-command fast path: capture ownership, then submit to Core once."""

    def __init__(self, bridge, *, matcher, client=None):
        if not callable(matcher):
            raise ValueError("A registered-command matcher is required")
        self.bridge = bridge
        self.matcher = matcher
        self.client = bridge.core_client if client is None else client

    def capture(self, request, *, audience):
        return self.bridge.capture(request, direct_match=self.matcher, audience=audience)

    async def submit(self, request, *, audience):
        """Decide the owner and, when direct, let Core run the one shared execution.

        A `direct` claim is not executed here: Core owns the registry, the durable request and
        the single execution, so the command entry and Core's explicit capability call for the
        same message version collapse into one run.
        """
        owner = self.capture(request, audience=audience)
        if owner != "direct":
            return dict(owner="companion", submitted=False, response=None)
        if self.client is None:
            raise Fault("dependency_unavailable")
        response = await self.client.call(DIRECT_ROUTE, request)
        self._check(response, request)
        if response["owner"] == "companion":
            self.bridge.hand_back(request)
        return dict(
            owner=response["owner"],
            submitted=response["owner"] == "direct",
            response=response,
        )

    @staticmethod
    def _check(response, request):
        if (
            not isinstance(response, dict)
            or response.get("request_id") != request["command"]["request_id"]
            or response.get("owner") not in OWNERS
        ):
            raise Fault("invalid_input")
        if response["owner"] == "direct" and not isinstance(response.get("request"), dict):
            raise Fault("invalid_input")


class BridgeDelivery:
    """Core-side delivery port for a bridge-owned direct reply.

    This is the in-process wiring of the same outbound path a deployment reaches over
    `POST /internal/v1/conversation/send`: `Bridge.send` with its own destination check,
    per-conversation serial position, `reply_id` de-duplication and receipt validation. A
    deployment that runs Core and the bridge as separate processes injects the existing
    `Sender` client instead; both land on this same method.
    """

    available = True

    def __init__(self, bridge, *, service="companion"):
        self.bridge, self.service = bridge, service

    async def send(self, request):
        return await self.bridge.send(self.service, request)

    async def reconcile(self, request):
        """Only a locally confirmed receipt is returned; an unknown one never resends."""
        return await self.bridge.reconcile(request)
