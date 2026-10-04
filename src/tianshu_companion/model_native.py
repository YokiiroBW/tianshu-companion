"""Assemble native completion deltas without converting tools or vision to text."""

import json

from .contracts import Fault


class Completion:
    def __init__(self):
        self.content = ""
        self.reasoning = ""
        self.tools = {}
        self.finish_reason = None
        self.usage = None

    def consume(self, chunk):
        if not isinstance(chunk, dict) or not isinstance(chunk.get("choices"), list):
            raise Fault("invalid_input", unknown=True)
        if chunk.get("usage") is not None:
            self.usage = chunk["usage"]
        emitted = ""
        for choice in chunk["choices"]:
            if choice.get("index") != 0:
                raise Fault("invalid_input", unknown=True)
            delta = choice.get("delta")
            if not isinstance(delta, dict):
                raise Fault("invalid_input", unknown=True)
            text = delta.get("content")
            if text is not None:
                if not isinstance(text, str):
                    raise Fault("invalid_input", unknown=True)
                self.content += text
                emitted += text
            reasoning = delta.get("reasoning_content")
            if reasoning is not None:
                if not isinstance(reasoning, str):
                    raise Fault("invalid_input", unknown=True)
                self.reasoning += reasoning
            if delta.get("refusal"):
                refusal = delta["refusal"]
                if not isinstance(refusal, str):
                    raise Fault("invalid_input", unknown=True)
                self.content += refusal
                emitted += refusal
            for part in delta.get("tool_calls") or []:
                index = part.get("index")
                if type(index) is not int or not 0 <= index < 16:
                    raise Fault("budget_exceeded", unknown=True)
                tool = self.tools.setdefault(
                    index, {"id": "", "type": "function", "function": {"name": "", "arguments": ""}}
                )
                if part.get("id"):
                    # Providers may repeat a stable ID rather than splitting it.
                    if tool["id"] and part["id"] != tool["id"]:
                        raise Fault("invalid_input", unknown=True)
                    tool["id"] = part["id"]
                if part.get("type", "function") != "function":
                    raise Fault("invalid_input", unknown=True)
                function = part.get("function") or {}
                for field in ("name", "arguments"):
                    value = function.get(field)
                    if value is not None:
                        if not isinstance(value, str):
                            raise Fault("invalid_input", unknown=True)
                        tool["function"][field] += value
                if len(tool["function"]["arguments"].encode()) > 100000:
                    raise Fault("budget_exceeded", unknown=True)
            if choice.get("finish_reason") is not None:
                self.finish_reason = choice["finish_reason"]
        if len(self.content.encode()) > 100000 or len(self.reasoning.encode()) > 200000:
            raise Fault("budget_exceeded", unknown=True)
        return emitted

    def message(self):
        if self.finish_reason not in {"stop", "tool_calls"}:
            raise Fault("dependency_unavailable", unknown=True)
        message = {"role": "assistant", "content": self.content or None}
        if self.tools:
            ordered = [self.tools[index] for index in sorted(self.tools)]
            if self.finish_reason != "tool_calls" or len({tool["id"] for tool in ordered}) != len(
                ordered
            ):
                raise Fault("invalid_input", unknown=True)
            for tool in ordered:
                if not tool["id"] or not tool["function"]["name"]:
                    raise Fault("invalid_input", unknown=True)
                try:
                    value = json.loads(tool["function"]["arguments"])
                except ValueError:
                    raise Fault("invalid_input", unknown=True) from None
                if not isinstance(value, dict):
                    raise Fault("invalid_input", unknown=True)
            message["tool_calls"] = ordered
        return message
