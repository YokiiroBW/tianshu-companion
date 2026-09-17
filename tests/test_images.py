"""Synthetic ComfyUI HTTP server and generated 1x1 PNG; never real GPU or private data."""

import asyncio
import copy
import hashlib
import json
import sqlite3
import struct
import threading
import time
import zlib
from contextlib import closing
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

import pytest

from test_life import setup
from tianshu_companion.images import ComfyUI, Images, Workflow
from tianshu_companion.store import IMAGE_TABLES, Store


def png():
    def chunk(kind, data):
        value = kind + data
        return struct.pack(">I", len(data)) + value + struct.pack(">I", zlib.crc32(value))

    return (
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", struct.pack(">IIBBBBB", 1, 1, 8, 2, 0, 0, 0))
        + chunk(b"IDAT", zlib.compress(b"\0\xff\0\0"))
        + chunk(b"IEND", b"")
    )


def workflow():
    graph = {
        "1": {"class_type": "CLIPTextEncode", "inputs": {"text": "fixed fictional artist"}},
        "2": {
            "class_type": "LoraLoaderModelOnly",
            "inputs": {"lora_name": "synthetic.safetensors", "strength_model": 1},
        },
        "3": {"class_type": "KSampler", "inputs": {"seed": 1, "steps": 20}},
        "4": {"class_type": "SaveImage", "inputs": {"filename_prefix": "synthetic"}},
        "5": {"class_type": "LoadImage", "inputs": {"image": "synthetic.png"}},
    }
    bindings = {
        "positive": dict(node="1", class_type="CLIPTextEncode", input="text"),
        "seed": dict(node="3", class_type="KSampler", input="seed", min=0, max=100),
        "reference": dict(node="5", class_type="LoadImage", input="image"),
    }
    return Workflow(graph, bindings, ["4"])


@pytest.fixture
def server():
    class State:
        posts = []
        pending = []
        running = []
        history = {}
        calls = []
        drop = False
        reject = False
        delay = 0
        data = png()
        race = False

    state = State()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def reply(self, value, code=200):
            data = value if isinstance(value, bytes) else json.dumps(value).encode()
            self.send_response(code)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            try:
                self.wfile.write(data)
            except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
                pass

        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            state.calls.append(("POST", self.path))
            if self.path == "/prompt":
                state.posts.append(body)
                if state.reject:
                    self.reply({"error": "synthetic invalid"}, 400)
                    return
                state.pending.append([0, body["prompt_id"]])
                if state.drop:
                    self.close_connection = True
                    return
                time.sleep(state.delay)
                self.reply({"prompt_id": body["prompt_id"]})
            elif self.path == "/queue":
                if state.race:
                    state.running = state.pending[:]
                state.pending = [x for x in state.pending if x[1] not in body["delete"]]
                self.reply({})
            else:
                self.reply({}, 404)

        def do_GET(self):
            state.calls.append(("GET", self.path))
            path = urlsplit(self.path).path
            if path.startswith("/history/"):
                key = path.removeprefix("/history/")
                self.reply({key: state.history[key]} if key in state.history else {})
            elif path == "/queue":
                self.reply(dict(queue_running=state.running, queue_pending=state.pending))
            elif path == "/view":
                self.reply(state.data)
            else:
                self.reply({}, 404)

    http = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=http.serve_forever, daemon=True)
    thread.start()
    yield state, "http://127.0.0.1:" + str(http.server_port)
    http.shutdown()
    thread.join()
    http.server_close()


def port(tmp_path, url, **options):
    life, _, _ = setup(Store(tmp_path / "synthetic.db"))
    transport = ComfyUI(url, timeout=0.3)
    images = Images(
        life, transport=transport, workflow=workflow(), staging=tmp_path / "staged", **options
    )
    images.put_outfit(
        "reading", description="Synthetic robe", prompt="blue robe", reference="synthetic.png"
    )
    images.select_outfit("a", "reading", expected=life.snapshot("a")["actor"]["version"])
    return images


def complete(state, job, descriptor=None):
    state.pending = []
    state.running = []
    state.history[job["prompt_id"]] = dict(
        status=dict(completed=True, status_str="success"),
        outputs={
            "4": dict(
                images=[descriptor or dict(filename="synthetic.png", subfolder="", type="output")]
            ),
            "unapproved": dict(
                images=[dict(filename="../secret.png", subfolder="", type="output")]
            ),
        },
    )


def test_workflow_immutable_and_validation():
    w = workflow()
    original = copy.deepcopy(w.graph)
    rendered = w.render(dict(positive="blue robe", seed=99))
    assert w.graph == original
    assert rendered["2"] == original["2"] and rendered["4"] == original["4"]
    assert rendered["1"]["inputs"]["text"] == "fixed fictional artist\nblue robe"
    for values in ({"seed": True}, {"seed": 101}, {"width": 200}, {"positive": ""}):
        with pytest.raises(ValueError):
            w.render(values)
    for graph in ({"nodes": []}, {"x": dict(class_type="x", inputs={"link": ["missing", 0]})}):
        with pytest.raises(ValueError):
            Workflow(graph, {}, ["4"])
    for change in ({"node": "missing"}, {"class_type": "LoadImage"}, {"input": "missing"}):
        bindings = copy.deepcopy(w.bindings)
        bindings["positive"].update(change)
        with pytest.raises(ValueError):
            Workflow(w.graph, bindings, w.outputs)
    bindings = copy.deepcopy(w.bindings)
    bindings["seed"].update(node="2", class_type="LoraLoaderModelOnly", input="strength_model")
    with pytest.raises(ValueError):
        Workflow(w.graph, bindings, w.outputs)


@pytest.mark.parametrize(
    "url", ["http://example.com", "http://user:pass@localhost", "https://host/path", "file:///x"]
)
def test_bad_origin(url):
    with pytest.raises(ValueError):
        ComfyUI(url)


def test_snapshot_idempotency_artifacts_and_restart(tmp_path, server):
    state, url = server

    async def run():
        images = port(tmp_path, url)
        head = images.store.source_head()
        job = images.request("r", "a", parameters={"seed": 7})
        images.life.set_activity(
            "a", "painting", expected=images.life.snapshot("a")["actor"]["version"]
        )
        images.put_outfit("reading", description="new", prompt="red robe", expected=1)
        assert images.request("r", "a", parameters={"seed": 7}) == job
        assert job["outfit"]["version"] == 1 and job["snapshot"]["actor"]["activity"] == "reading"
        assert images.request("s", "a")["outfit"]["version"] == 2
        with pytest.raises(ValueError):
            images.request("r", "b", parameters={"seed": 7})
        await images.work()
        assert len(state.posts) == 1
        images.recover()
        assert images.get("r")["state"] == "unknown"
        # Actual database reopen preserves prompt identity and graph.
        await images.transport.close()
        images.store.close()
        store = Store(tmp_path / "synthetic.db")
        images.life.store = store
        images = Images(
            images.life, transport=ComfyUI(url), workflow=workflow(), staging=tmp_path / "staged"
        )
        complete(state, job)
        # s is unsubmitted and scheduled first; cancel it to isolate recovered r.
        images.cancel("s")
        await images.work()
        done = images.get("r")
        assert done["state"] == "completed" and len(state.posts) == 1
        artifact = done["artifacts"][0]
        assert artifact["sha256"] == hashlib.sha256(png()).hexdigest()
        assert (tmp_path / "staged" / artifact["staging_name"]).read_bytes() == png()
        assert artifact["archived"] is False and done["fictional"] is True
        assert images.life.snapshot("a")["actor"]["activity"] == "painting"
        assert store.source_head() == head
        assert not any("secret" in path for _, path in state.calls)
        await images.transport.close()
        store.close()

    asyncio.run(run())


def test_unknown_does_not_resubmit_and_cancel_pending(tmp_path, server):
    state, url = server
    state.drop = True

    async def run():
        images = port(tmp_path, url)
        job = images.request("r", "a")
        await images.work()
        assert images.get("r")["state"] == "unknown"
        assert images.request("r", "a")["prompt_id"] == job["prompt_id"]
        await images.work()
        assert images.get("r")["state"] == "queued" and len(state.posts) == 1
        images.cancel("r")
        await images.work()
        assert images.get("r")["state"] == "cancelled"
        assert not any(path == "/interrupt" for _, path in state.calls)
        await images.transport.close()
        images.store.close()

    asyncio.run(run())


@pytest.mark.parametrize("race", [False, True])
def test_running_cancel_is_intent_only(tmp_path, server, race):
    state, url = server

    async def run():
        images = port(tmp_path, url)
        job = images.request("r", "a")
        await images.work()
        if race:
            state.race = True
        else:
            state.running, state.pending = state.pending, []
        images.cancel("r")
        await images.work()
        assert images.get("r")["state"] != "cancelled"
        await images.work()
        assert images.get("r")["state"] == "running"
        complete(state, job)
        await images.work()
        assert images.get("r")["state"] == "completed" and images.get("r")["cancel_requested"]
        await images.transport.close()
        images.store.close()

    asyncio.run(run())


@pytest.mark.parametrize(
    "descriptor",
    [
        dict(filename="../secret.png", subfolder="", type="output"),
        dict(filename="a.png", subfolder="../secret", type="output"),
        dict(filename="a.png", subfolder="", type="input"),
        *[
            dict(filename="secret.png" + prefix + suffix, subfolder="", type="output")
            for prefix in (" ", "", "X")
            for suffix in ("[input]", "[temp]", "[output]")
        ],
        dict(filename="C:\\secret.png", subfolder="", type="output"),
    ],
)
def test_artifact_traversal_rejected_before_download(tmp_path, server, descriptor):
    state, url = server

    async def run():
        images = port(tmp_path, url)
        job = images.request("r", "a")
        await images.work()
        complete(state, job, descriptor)
        await images.work()
        assert images.get("r")["state"] == "unknown"
        assert not any(path.startswith("/view") for _, path in state.calls)
        assert not list((tmp_path / "staged").glob("*"))
        await images.transport.close()
        images.store.close()

    asyncio.run(run())


@pytest.mark.parametrize("mode", ["size", "type", "crc", "storage", "count"])
def test_artifact_budgets(tmp_path, server, mode):
    state, url = server

    async def run():
        options = (
            {"max_bytes": 32}
            if mode == "size"
            else {"max_storage": 32}
            if mode == "storage"
            else {}
        )
        images = port(tmp_path, url, **options)
        job = images.request("r", "a")
        await images.work()
        complete(state, job)
        if mode == "type":
            state.data = b"not an image"
        if mode == "crc":
            state.data = png()[:-1] + b"X"
        if mode == "count":
            state.history[job["prompt_id"]]["outputs"]["4"]["images"] *= 5
        await images.work()
        assert images.get("r")["state"] == "unknown"
        assert not list((tmp_path / "staged").glob("*"))
        await images.transport.close()
        images.store.close()

    asyncio.run(run())


def test_budget_local_cancel_rejection_and_absent_history(tmp_path, server):
    state, url = server

    async def run():
        images = port(tmp_path, url, max_pending=1)
        images.request("r", "a")
        with pytest.raises(ValueError):
            images.request("s", "a")
        images.cancel("r")
        await images.work()
        assert not state.posts
        state.reject = True
        images.request("s", "a")
        await images.work()
        assert images.get("s")["state"] == "failed"
        state.reject = False
        images.request("t", "a")
        await images.work()
        state.pending = []
        for _ in range(2):
            await images.work()
        assert images.get("t")["state"] == "unknown" and len(state.posts) == 2
        await images.transport.close()
        images.store.close()

    asyncio.run(run())


def test_image_network_does_not_block_chat_or_duplicate_work(tmp_path, server):
    from support import Harness

    state, url = server
    state.delay = 0.15

    async def run():
        images = port(tmp_path, url)
        images.request("r", "a")
        task = asyncio.create_task(images.work())
        await asyncio.sleep(0.02)
        await images.work()  # bounded concurrency, no second POST
        h = Harness()
        try:
            await h.ingest(text="Synthetic ordinary chat during image submission")
            h.clock.advance(6)
            await asyncio.wait_for(h.cycles(), 0.1)
            assert h.sender.calls and h.turns()[0]["phase"] == "sent"
            assert not task.done()
            images.cancel("r")  # cancellation during submission must survive later persistence
            await task
            assert images.get("r")["cancel_requested"] and len(state.posts) == 1
        finally:
            await h.core.close()
            await images.transport.close()
            images.store.close()

    asyncio.run(run())


def test_v3_migration_backup_rollback_and_source_preservation(tmp_path):
    path = tmp_path / "synthetic.db"
    store = Store(path)
    head = store.source_head()
    store.close()
    with closing(sqlite3.connect(path)) as db:
        for table in IMAGE_TABLES:
            db.execute(f"DROP TABLE {table}")
        db.execute("PRAGMA user_version=3")
        db.execute("CREATE TABLE image_outfits_queue (id TEXT)")
        db.commit()
    with pytest.raises(sqlite3.OperationalError):
        Store(path)
    with closing(sqlite3.connect(path)) as db:
        assert db.execute("PRAGMA user_version").fetchone()[0] == 3
        assert not db.execute("SELECT name FROM sqlite_master WHERE name='image_jobs'").fetchone()
        db.execute("DROP TABLE image_outfits_queue")
        db.commit()
    with closing(Store(path)) as store:
        assert store.source_head() == head
        assert store.db.execute("PRAGMA user_version").fetchone()[0] == 8
    assert len(list(tmp_path.glob("*.pre-images-v4-*.bak"))) == 2  # attempt and retry
    # Both attempts started below v7, so the persona step is crossed inside those same
    # migrations: a multi-version jump takes one recovery backup, at the highest structural
    # step, and never a second one for an earlier step in the same run.
    assert not list(tmp_path.glob("*.pre-persona-v8-*.bak"))


def test_changed_endpoint_does_not_query_old_prompt_or_starve_new_job(tmp_path, server):
    state, url = server

    async def run():
        images = port(tmp_path, url)
        job = images.request("a-old", "a")
        job["endpoint"] = "synthetic different origin"
        images.store.put("image_jobs", job)
        new = images.request("b-new", "a")
        await images.work()
        assert len(state.posts) == 1 and state.posts[0]["prompt_id"] == new["prompt_id"]
        assert images.get("a-old")["submitted"] is False
        assert not any(job["prompt_id"] in path for _, path in state.calls)
        await images.transport.close()
        images.store.close()

    asyncio.run(run())


@pytest.mark.parametrize(
    "filename",
    [
        "synthetic.png",
        "synthetic[output].png",
        "synthetic.png [INPUT]",
        "synthetic.png%20%5Binput%5D",
    ],
)
def test_view_only_forwards_allowed_fields_and_preserves_literal_filename(
    tmp_path, server, filename
):
    state, url = server

    async def run():
        images = port(tmp_path, url)
        job = images.request("r", "a")
        complete_descriptor = dict(
            filename=filename,
            subfolder="synthetic folder",
            type="output",
            preview="jpeg;1",
            channel="a",
            unknown="synthetic extension",
        )
        await images.work()
        complete(state, job, complete_descriptor)
        await images.work()
        assert images.get("r")["state"] == "completed"
        requests = [path for method, path in state.calls if path.startswith("/view")]
        assert len(requests) == 1
        assert parse_qs(urlsplit(requests[0]).query, keep_blank_values=True) == {
            "filename": [filename],
            "subfolder": ["synthetic folder"],
            "type": ["output"],
        }
        assert images.get("r")["artifacts"][0]["sha256"] == hashlib.sha256(png()).hexdigest()
        await images.transport.close()
        images.store.close()

    asyncio.run(run())
