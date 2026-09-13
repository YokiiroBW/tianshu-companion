"""Thin SDK boundary, independently persisted ingress and send attempts."""

from .bridge import Bridge, normalize_onebot, normalize_telegram

__all__ = ["Bridge", "normalize_onebot", "normalize_telegram"]
