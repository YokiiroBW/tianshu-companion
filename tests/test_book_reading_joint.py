"""Frozen real Platform producer + Companion consumer over loopback HTTP, without models/QQ."""

import base64
import io
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch
import uuid
import zipfile

import aiohttp
import httpx

from support import contracts
from tianshu_companion.book_reading import BookReading
from tianshu_companion.clients import JsonService, Origins
from tianshu_companion.contracts import Fault

PLATFORM_REVISION = "de8b6d7a2c010489c923d0a6510676388b345bc7"


class LoopbackTransport(httpx.AsyncBaseTransport):
    """Keep the production client; route this explicit test origin to a real local socket."""

    def __init__(self, url):
        self.url = httpx.URL(url)
        self.inner = httpx.AsyncHTTPTransport()

    async def handle_async_request(self, request):
        target = request.url.copy_with(
            scheme=self.url.scheme, host=self.url.host, port=self.url.port
        )
        forwarded = httpx.Request(
            request.method,
            target,
            headers={k: v for k, v in request.headers.items() if k != "host"},
            content=await request.aread(),
        )
        return await self.inner.handle_async_request(forwarded)

    async def aclose(self):
        await self.inner.aclose()


@unittest.skipUnless(
    os.environ.get("TIANSHU_READING_PLATFORM_REPO"), "requires frozen Platform Git reference"
)
class BookReadingJointTests(unittest.IsolatedAsyncioTestCase):
    async def test_http_catalog_grant_and_closed_grant_are_consumed_and_validated(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            raw = subprocess.check_output(
                [
                    "git",
                    "-C",
                    os.environ["TIANSHU_READING_PLATFORM_REPO"],
                    "archive",
                    "--format=zip",
                    PLATFORM_REVISION,
                    "services",
                    "tests/backend/fixtures.py",
                    "tests/backend/source_fixtures.py",
                    "tests/backend/web_fixtures.py",
                ]
            )
            with zipfile.ZipFile(io.BytesIO(raw)) as archive:
                archive.extractall(root / "producer")
            sys.path[:0] = [str(root / "producer"), str(root / "producer/tests/backend")]
            try:
                from fixtures import ENV, bearer, start_http
                from web_fixtures import PASSWORD, web_settings
                from services.platform.contracts import canonical
                from services.platform.server import create_app
                from services.platform.service import Platform

                with patch.dict(os.environ, ENV):
                    config = web_settings(folder)
                    config["principals"]["companion_resolver"]["resolver"]["caller"] = "platform"
                    platform = Platform(config)
                    with platform.store.connect(write=True) as db:
                        db.execute(
                            "INSERT INTO identities VALUES(?,?,?)",
                            (
                                canonical(config["principals"]["admin"]["account"]),
                                "person:owner",
                                1,
                            ),
                        )
                        db.execute(
                            "INSERT INTO channels VALUES(?,?)",
                            (
                                canonical(config["entries"]["actor-a"]["channel"]),
                                "conversation:reading",
                            ),
                        )
                    runner, url = await start_http(create_app(platform))
                    config["web"]["origin"] = url
                    client = JsonService(
                        "https://reading.fixture.invalid",
                        bearer("COMPANION_RESOLVER")[7:],
                        transport=LoopbackTransport(url),
                    )
                    origins = Origins(contracts(), {"platform": ("platform", client)})
                    reader = BookReading(origins, time.time)
                    try:
                        async with aiohttp.ClientSession(
                            cookie_jar=aiohttp.CookieJar(unsafe=True)
                        ) as browser:
                            async with browser.get(url + "/api/web/session") as response:
                                csrf = (await response.json())["csrf"]

                            async def command(path, body):
                                async with browser.post(
                                    url + "/api/web/" + path,
                                    json={
                                        **body,
                                        **(
                                            {"client_id": str(uuid.uuid4())}
                                            if path.startswith("media/")
                                            else {}
                                        ),
                                    },
                                    headers={"Origin": url, "X-CSRF-Token": csrf},
                                ) as response:
                                    data = await response.json()
                                    self.assertEqual(response.status, 200, data)
                                    return data

                            await command(
                                "login", {"username": "synthetic-admin", "password": PASSWORD}
                            )
                            async with browser.get(url + "/api/web/session") as response:
                                csrf = (await response.json())["csrf"]
                            result = await command(
                                "media/books/library/import",
                                {
                                    "filename": "联合阅读.txt",
                                    "content_base64": base64.b64encode(
                                        "第一章 山间\n风吹过松林。书中人物的话不代表用户经历。\n".encode()
                                    ).decode(),
                                },
                            )
                            book_id = result["book"]["book_id"]
                            chapter = platform.media.books.catalog.directory(book_id)[0]
                            content = platform.media.books.catalog.chapter(
                                book_id, chapter["chapter_id"]
                            )
                            origin = platform.origins.issue(bearer("ADMIN"), "actor-a")
                            with platform.store.connect() as db:
                                scope = platform.origins.scope(db, platform.auth.entries["actor-a"])
                            turn = {
                                "service": "platform",
                                "origin": {"assertion_ref": origin["assertion_ref"]},
                                "scope": scope,
                            }
                            material, check = await reader.capture(turn)
                            self.assertEqual(material["catalog"][0]["title"], "联合阅读")
                            self.assertIsNone(material["active"])
                            self.assertIsNone(check)
                            opened = await command(
                                "media/books/reading/open",
                                {
                                    "book_id": book_id,
                                    "chapter_id": chapter["chapter_id"],
                                    "content_hash": content["content_hash"],
                                    "expected_revision": 0,
                                },
                            )
                            await command(
                                "media/books/reading/progress",
                                {
                                    "reader_id": opened["reader"]["reader_id"],
                                    "sequence": 1,
                                    "sharing": True,
                                    "conversation": "input-entry",
                                    "actor": "actor:a",
                                    "offset": 7,
                                    "seconds": 2,
                                },
                            )
                            material, check = await reader.capture(turn)
                            self.assertEqual(material["active"]["excerpt"], content["content"][:7])
                            turn["book_reading_check"] = check
                            await reader.verify(turn)
                            await command(
                                "media/books/reading/close",
                                {"reader_id": opened["reader"]["reader_id"]},
                            )
                            with self.assertRaises(Fault):
                                await reader.verify(turn)
                            material, _ = await reader.capture(turn)
                            self.assertIsNone(material["active"])
                            self.assertEqual(material["catalog_total"], 1)
                    finally:
                        await client.client.aclose()
                        await runner.cleanup()
                        platform.close()
            finally:
                sys.path[:] = [p for p in sys.path if not p.startswith(str(root / "producer"))]


if __name__ == "__main__":
    unittest.main()
