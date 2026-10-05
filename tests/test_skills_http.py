"""Actual Companion routes and HTTP adapter serialization with isolated transport peers."""

import asyncio
import json

import httpx
import pytest

from support import Harness
from tianshu_companion.app import create_app
from tianshu_companion.contracts import Fault
from tianshu_companion.skills import gscore, sources


def test_skills_registered_http_read_manage_shape_and_cas():
    async def run():
        h = Harness()
        try:
            h.core.recover()
            app = create_app(h.core, {"platform": "synthetic-platform"})
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://fixture"
            ) as client:
                body = dict(
                    schema_version=1, request_id="read", actor_id="actor:a", resource="list"
                )
                path = "/internal/v1/skills/read"
                assert (await client.post(path, json=body)).status_code == 401
                headers = {"Authorization": "Bearer synthetic-platform"}
                result = (await client.post(path, json=body, headers=headers)).json()
                h.contracts.check("skills#response", result)
                assert result["result"]["actor_version"] == 1
                assert all(
                    "handler_id" not in skill and "handler_id" in skill["definition"]
                    for skill in result["result"]["skills"]
                )
                command = dict(
                    schema_version=1,
                    request_id="disable",
                    actor_id="actor:a",
                    expected_version=1,
                    operation="skill.disable",
                    value=dict(skill_id="image.generate"),
                )
                response = await client.post(
                    "/internal/v1/skills/manage", json=command, headers=headers
                )
                assert (
                    response.status_code == 200 and response.json()["result"]["actor_version"] == 2
                )
                command.update(request_id="stale", operation="skill.enable")
                conflict = await client.post(
                    "/internal/v1/skills/manage", json=command, headers=headers
                )
                assert conflict.status_code == 409 and conflict.json()["current_version"] == 2
        finally:
            await h.core.close()

    asyncio.run(run())


def test_http_mapping_and_source_no_redirect_token_origin(monkeypatch):
    async def run():
        h = Harness()
        try:
            calls = []

            async def handle(request):
                calls.append(request)
                if request.url.path == "/catalog.json":
                    return httpx.Response(
                        302, headers={"Location": "https://foreign.invalid/catalog.json"}
                    )
                assert request.headers["X-WS-Token"] == "synthetic-secret"
                assert json.loads(request.content)["content"][0]["data"] == "ww长离攻略"
                return httpx.Response(200, json=dict(status_code=-100, data=None))

            constructor = httpx.AsyncClient

            def client(**kwargs):
                assert kwargs["follow_redirects"] is False
                return constructor(transport=httpx.MockTransport(handle), **kwargs)

            monkeypatch.setattr(httpx, "AsyncClient", client)
            response = await gscore.request(
                dict(base_url="http://fixture.invalid:28765"),
                "synthetic-secret",
                dict(content=[dict(type="text", data="ww长离攻略")]),
            )
            assert response == dict(status_code=-100, data=None)

            class Credentials:
                async def call(self, path, value):
                    assert value["purpose"] == "companion.skills"
                    assert value["audience"] == "http://fixture.invalid:28765"
                    return dict(
                        schema_version=1,
                        request_id=value["request_id"],
                        credential_ref=value["credential_ref"],
                        token="synthetic-secret",
                    )

            h.core.image_backend.credentials = Credentials()
            with pytest.raises(Fault, match="dependency_unavailable"):
                await sources.fetch(
                    h.core.skills,
                    dict(
                        credential_ref="fixture-ref",
                        manifest_url="http://fixture.invalid:28765/catalog.json",
                    ),
                )
            assert (
                len(calls) == 2 and calls[1].headers["Authorization"] == "Bearer synthetic-secret"
            )
            assert not any(request.url.host == "foreign.invalid" for request in calls)
            for url in (
                "https://user:secret@fixture.invalid",
                "https://fixture.invalid?token=secret",
            ):
                with pytest.raises(Fault):
                    sources.origin(url)
        finally:
            await h.core.close()

    asyncio.run(run())
