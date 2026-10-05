"""Shared bounded raster validation for actual received originals."""

import struct
import zlib


def png(data, maximum):
    if len(data) > maximum:
        raise ValueError("Artifact byte budget exceeded")
    if len(data) < 33 or data[:16] != b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR":
        raise ValueError("Expected PNG")
    width, height = struct.unpack(">II", data[16:24])
    if not 0 < width <= 8192 or not 0 < height <= 8192 or width * height > 32_000_000:
        raise ValueError("Image pixel budget exceeded")
    offset, has_data, ended = 8, False, False
    while offset + 12 <= len(data):
        size = int.from_bytes(data[offset : offset + 4], "big")
        end = offset + 12 + size
        if end > len(data):
            raise ValueError("Truncated PNG")
        chunk = data[offset + 4 : end - 4]
        if zlib.crc32(chunk) != int.from_bytes(data[end - 4 : end], "big"):
            raise ValueError("PNG CRC mismatch")
        has_data |= chunk[:4] == b"IDAT"
        offset = end
        if chunk[:4] == b"IEND":
            ended = size == 0 and end == len(data)
            break
    if not has_data or not ended:
        raise ValueError("Incomplete PNG")
    return bytes(data), width, height
