"""A real, incompressible PNG original above 2 MiB; no model or network source."""

import base64
import hashlib
import random
import struct
import zlib


def original_png():
    width = height = 1024
    rng = random.Random(20261004)
    pixels = b"".join(b"\0" + rng.randbytes(width * 3) for _ in range(height))

    def chunk(kind, payload):
        return (
            struct.pack(">I", len(payload))
            + kind
            + payload
            + struct.pack(">I", zlib.crc32(kind + payload))
        )

    raw = (
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0))
        + chunk(b"IDAT", zlib.compress(pixels))
        + chunk(b"IEND", b"")
    )
    reference = {
        "owner": "memory",
        "object_id": "original:synthetic-png",
        "version": 1,
        "kind": "image",
        "sha256": hashlib.sha256(raw).hexdigest(),
        "sources": [],
        "coverage": {"unit": "bytes", "start": 0, "end": len(raw), "total": len(raw)},
    }
    media = {
        "content_ref": reference,
        "media_type": "image/png",
        "encoding": "base64",
        "data": base64.b64encode(raw).decode(),
        "sha256": reference["sha256"],
    }
    return raw, reference, media
