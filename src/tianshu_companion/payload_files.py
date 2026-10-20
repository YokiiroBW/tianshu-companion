"""File-backed large payloads; SQL retains identity and immutable wire digests."""

import base64
import copy
import json

from tianshu_storage import FileStore

from .contracts import Fault, canonical, digest

MATERIAL_BYTES = 64 * 1024


class PayloadFiles:
    def __init__(self, root):
        self.files = FileStore(root)

    def freeze(self, table, item):
        stored = copy.deepcopy(item)
        if table == "replies" and stored.get("v2_request"):
            request = stored["v2_request"]
            changed = False
            for segment in request.get("segments", []):
                for media in segment.get("media", []):
                    if media.get("encoding") == "base64" and "data" in media:
                        try:
                            data = base64.b64decode(media["data"], validate=True)
                        except (ValueError, TypeError):
                            raise Fault("invalid_input") from None
                        reference = self.files.put_bytes(data, media_type=media["media_type"])
                        media.pop("data")
                        media["payload_file"] = reference.as_dict()
                        changed = True
            if changed:
                stored["v2_request_digest"] = digest(item["v2_request"])
        if table == "write_materials" and "material" in stored:
            content = canonical(stored["material"])
            if len(content.encode("utf-8")) >= MATERIAL_BYTES:
                stored["material_file"] = self.files.put_text(content).as_dict()
                stored.pop("material")
        return stored

    def restore(self, table, item):
        if item is None:
            return None
        try:
            if table == "replies" and item.get("v2_request_digest"):
                request = item["v2_request"]
                for segment in request.get("segments", []):
                    for media in segment.get("media", []):
                        if "payload_file" in media:
                            media["data"] = base64.b64encode(
                                self.files.read_bytes(media.pop("payload_file"))
                            ).decode("ascii")
                if digest(request) != item.pop("v2_request_digest"):
                    raise ValueError("Frozen request mismatch")
            if table == "write_materials" and "material_file" in item:
                item["material"] = json.loads(self.files.read_text(item.pop("material_file")))
        except (OSError, ValueError, KeyError, TypeError):
            raise Fault("dependency_unavailable") from None
        return item
