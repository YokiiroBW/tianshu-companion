"""Persistent ComfyUI connection settings; only the transport holds resolved secrets."""

import httpx

from .contracts import Fault, digest
from .images import ComfyUI, Workflow
from .life_work import require_version


class ImageBackend:
    KEY = "image-backend:current"

    def __init__(self, images, credential_client=None):
        self.images, self.store = images, images.store
        self.credentials = credential_client
        self.checked = None
        self.models = []
        self.error = None
        self.transports = []
        self.runtime = None
        from .image_catalog import ImageCatalog

        self.catalog = ImageCatalog(self)

    def current(self):
        return self.store.get("metadata", self.KEY)

    def reference(self, actor):
        return self.store.get("metadata", "image-reference:" + actor) or dict(
            id="image-reference:" + actor,
            version=1,
            actor_id=actor,
            content_ref=None,
            scope=None,
            state="not_configured",
        )

    async def authorize_reference(self, actor, reference, scope, query):
        if reference is None:
            return
        if scope is not None:
            await self.runtime.authorize_read(
                "platform", dict(actor_id=actor, scope=scope, query=query)
            )
        data, media_type = await self.images.original_reader(
            actor, scope, reference, query, actor_owned=scope is None, allow_unregistered=True
        )
        if media_type not in {"image/png", "image/jpeg", "image/webp"}:
            raise Fault("invalid_input")
        return data, media_type

    def remember_reference(self, actor, reference, scope, query):
        if reference is None or reference["owner"] != "memory":
            return
        key = "content:" + digest(reference)
        if not self.store.get("life_content_refs", key):
            self.store.put(
                "life_content_refs",
                dict(
                    id=key,
                    actor_id=actor,
                    conversation_id=actor,
                    scope=scope,
                    version=1,
                    state="available",
                    content_ref=reference,
                    query=query,
                    operation_ref=None,
                    acquired_at=self.images.life.clock(),
                ),
            )

    def configure_reference(self, actor, value, expected):
        require_version(self.reference(actor), expected)
        self.remember_reference(actor, value["content_ref"], value["scope"], value["query"])
        item = dict(
            id="image-reference:" + actor,
            version=expected + 1,
            actor_id=actor,
            content_ref=value["content_ref"],
            scope=value["scope"],
            state="configured" if value["content_ref"] else "not_configured",
        )
        self.store.put("metadata", item)
        return item

    async def prepare_job(self, actor, value):
        workflow, _ = self.catalog.selected(actor)
        if not self.images.transport or not workflow:
            raise Fault("dependency_unavailable")
        references = []
        scope, query = value.get("scope"), value.get("query")
        if value.get("edit_source_ref"):
            references.append(dict(content_ref=value["edit_source_ref"], scope=scope, query=query))
        elif value.get("edit_source_id"):
            _, media = self.images.life.album.read(actor, value["edit_source_id"], scope)
            references.append(dict(content_ref=media["content_ref"], scope=scope, query=query))
        identity = self.reference(actor)
        if identity and identity["content_ref"]:
            references.append(
                dict(content_ref=identity["content_ref"], scope=identity["scope"], query=query)
            )
        current = self.store.get("life_actors", actor)
        outfit = self.store.get("image_outfits", value.get("outfit_id") or current["outfit_ref"])
        if outfit and isinstance(outfit["reference"], dict):
            references.append(
                dict(content_ref=outfit["reference"], scope=outfit["source_scope"], query=query)
            )
        prepared = []
        for reference in references:
            # A private original remains private when used in a generated derivative.
            if reference["scope"] is not None and reference["scope"] != scope:
                raise Fault("forbidden")
            data, media_type = await self.authorize_reference(
                actor,
                **dict(
                    reference=reference["content_ref"],
                    scope=reference["scope"],
                    query=reference["query"],
                ),
            )
            extension = {"image/png": ".png", "image/jpeg": ".jpg", "image/webp": ".webp"}[
                media_type
            ]
            reference["filename"] = "tianshu_" + reference["content_ref"]["sha256"] + extension
            # Do not upload here: the durable job owns submission and rechecks the owner first.
            prepared.append(reference)
        return prepared

    async def resolve(self, value):
        from .service_credentials import resolve

        token = await resolve(
            self.credentials, value["credential_ref"], value["base_url"], "companion.images"
        )
        return ComfyUI(value["base_url"], token=token, credential_ref=value["credential_ref"])

    async def prepare(self, value, expected):
        require_version(self.current(), expected)
        if value["profile"] != "standard_sd":
            raise Fault("invalid_input")
        # Validate the fixed origin even when disabled or the credential server is offline.
        validation = ComfyUI(value["base_url"])
        await validation.close()
        transport = None
        models, checked_at, error = [], None, None
        if value["enabled"]:
            try:
                transport = await self.resolve(value)
                models = await self._models(transport)
                checked_at = self.images.life.clock()
                if value["checkpoint"] not in models:
                    raise Fault("not_found")
            except (Fault, ValueError, OSError, httpx.HTTPError) as exception:
                error = exception.code if isinstance(exception, Fault) else "dependency_unavailable"
        return dict(
            value=value, transport=transport, models=models, checked_at=checked_at, error=error
        )

    def apply(self, prepared, expected):
        require_version(self.current(), expected)
        value = prepared["value"]
        item = dict(
            value,
            id=self.KEY,
            version=expected + 1,
            state="disabled"
            if not value["enabled"]
            else "unreachable"
            if prepared["error"]
            else "configured",
        )
        self.store.put("metadata", item)
        self.checked, self.models, self.error = (
            prepared["checked_at"],
            prepared["models"],
            prepared["error"],
        )
        self.images.transport = prepared["transport"] if item["state"] == "configured" else None
        self.images.workflow = (
            Workflow.standard(value["checkpoint"]) if item["state"] == "configured" else None
        )
        self.images.standard_checkpoint = (
            value["checkpoint"] if item["state"] == "configured" else None
        )
        if prepared["transport"]:
            self.transports.append(prepared["transport"])
        return item

    async def restore(self):
        current = self.current()
        if current:
            value = {
                name: current[name]
                for name in ("base_url", "credential_ref", "profile", "checkpoint", "enabled")
            }
            prepared = await self.prepare(value, current["version"])
            self.checked, self.models, self.error = (
                prepared["checked_at"],
                prepared["models"],
                prepared["error"],
            )
            self.images.transport = (
                prepared["transport"] if not prepared["error"] and value["enabled"] else None
            )
            self.images.workflow = (
                Workflow.standard(value["checkpoint"]) if self.images.transport else None
            )
            self.images.standard_checkpoint = value["checkpoint"] if self.images.transport else None
            if prepared["transport"]:
                self.transports.append(prepared["transport"])

    async def _models(self, transport):
        response = await transport.json("GET", "/object_info/CheckpointLoaderSimple")
        try:
            names = response["CheckpointLoaderSimple"]["input"]["required"]["ckpt_name"][0]
        except (KeyError, IndexError, TypeError):
            raise Fault("dependency_unavailable") from None
        if (
            not isinstance(names, list)
            or len(names) > 1000
            or any(not isinstance(name, str) or not 1 <= len(name) <= 128 for name in names)
        ):
            raise Fault("dependency_unavailable")
        return names

    async def view(self):
        current = self.current()
        if current is None:
            return dict(
                id=self.KEY,
                version=1,
                state="not_configured",
                base_url=None,
                credential_ref=None,
                profile=None,
                checkpoint=None,
                models=[],
                checked_at=None,
                error_code=None,
            )
        if current["enabled"] and (
            self.checked is None or self.images.life.clock() - self.checked > 30
        ):
            try:
                if self.images.transport is None:
                    self.images.transport = await self.resolve(current)
                    self.transports.append(self.images.transport)
                self.models = await self._models(self.images.transport)
                self.images.workflow = Workflow.standard(current["checkpoint"])
                self.images.standard_checkpoint = current["checkpoint"]
                self.error = None if current["checkpoint"] in self.models else "not_found"
            except Exception:
                self.error = "dependency_unavailable"
            self.checked = self.images.life.clock()
            if self.error:
                self.images.workflow = None
                self.images.standard_checkpoint = None
        return dict(
            id=self.KEY,
            version=current["version"],
            state="disabled"
            if not current["enabled"]
            else "unreachable"
            if self.error
            else "configured",
            **{
                name: current[name]
                for name in ("base_url", "credential_ref", "profile", "checkpoint")
            },
            models=self.models,
            checked_at=self.checked,
            error_code=self.error,
        )

    async def close(self):
        for transport in self.transports:
            await transport.close()
