"""Thin SDK boundary, independently persisted ingress and send attempts."""

__all__ = ["Bridge", "normalize_onebot", "normalize_telegram"]


def __getattr__(name):
    # The historical bridge depends on Companion Core. Adapter mode is a
    # standalone host plugin and must not import that optional dependency.
    if name in __all__:
        from .bridge import Bridge, normalize_onebot, normalize_telegram
        return {"Bridge": Bridge, "normalize_onebot": normalize_onebot,
                "normalize_telegram": normalize_telegram}[name]
    raise AttributeError(name)
