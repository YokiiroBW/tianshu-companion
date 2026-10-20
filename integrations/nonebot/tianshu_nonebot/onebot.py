"""Standalone Tianshu OneBot host using the existing SDK and durable adapter."""

import argparse
import json
import os
from pathlib import Path
import sqlite3


def parser():
    result = argparse.ArgumentParser(description="Tianshu OneBot v11 channel service")
    result.add_argument("--host", default="127.0.0.1")
    result.add_argument("--port", type=int, default=18765)
    result.add_argument("--data-dir", type=Path, default=Path("data/tianshu_onebot"))
    result.add_argument("--onebot-token-env", default="TIANSHU_ONEBOT_TOKEN")
    result.add_argument("--storage-config", type=Path, help="JSON containing storage_backends")
    result.add_argument(
        "--show-key", action="store_true", help="Display the local Platform connection key"
    )
    result.add_argument(
        "--print-config", action="store_true", help="Display non-secret connection information"
    )
    return result


def create_host(*, host, port, data_dir, access_token, storage_backends=None):
    if not access_token or any(char.isspace() for char in access_token):
        raise ValueError("A OneBot access token is required")
    if not 1 <= port <= 65535:
        raise ValueError("Port must be between 1 and 65535")
    import nonebot
    from fastapi.responses import JSONResponse
    from nonebot.adapters.onebot.v11 import Adapter

    nonebot.init(
        driver="~fastapi",
        host=host,
        port=port,
        log_level="WARNING",
        onebot_access_token=access_token,
        tianshu_adapter_kind="onebot",
        tianshu_adapter_data_dir=str(data_dir),
        storage_backends=storage_backends or {},
        _env_file=None,
    )
    nonebot.get_driver().register_adapter(Adapter)
    if nonebot.load_plugin("tianshu_nonebot.adapter_plugin") is None:
        raise RuntimeError("Tianshu adapter could not be loaded")
    app = nonebot.get_asgi()

    @app.get("/health/live", include_in_schema=False)
    async def live():
        return {"service": "tianshu-onebot", "status": "alive"}

    @app.get("/health/ready", include_in_schema=False)
    async def ready():
        connected = bool(nonebot.get_bots())
        return JSONResponse(
            {"service": "tianshu-onebot", "status": "ready" if connected else "offline"},
            status_code=200 if connected else 503,
        )

    return app


def main():
    command = parser()
    args = command.parse_args()
    if not 1 <= args.port <= 65535:
        command.error("port must be between 1 and 65535")
    data = args.data_dir.resolve()
    settings = (
        json.loads(args.storage_config.read_text(encoding="utf-8")) if args.storage_config else {}
    )
    if args.show_key:
        from .storage import read_access_key

        database = data / "adapter.sqlite3"
        try:
            print(read_access_key(database, settings))
        except (OSError, ValueError, RuntimeError, sqlite3.Error):
            command.error("current backend or initialized connection key is unavailable")
        return
    if args.print_config:
        print(
            json.dumps(
                dict(
                    service="tianshu-onebot",
                    host=args.host,
                    port=args.port,
                    data_directory=str(data),
                    adapter="onebot",
                    platform_http_path="/tianshu/adapter/v1",
                    onebot_websocket_path="/onebot/v11/ws",
                    token_environment=args.onebot_token_env,
                )
            )
        )
        return
    access_token = os.environ.get(args.onebot_token_env, "")
    token_file = os.environ.get("TIANSHU_ONEBOT_TOKEN_FILE")
    if not access_token and token_file:
        access_token = Path(token_file).read_text(encoding="utf-8").strip()
    if not access_token:
        command.error(
            "configure the OneBot token environment variable or TIANSHU_ONEBOT_TOKEN_FILE"
        )
    create_host(
        host=args.host,
        port=args.port,
        data_dir=data,
        access_token=access_token,
        storage_backends=settings.get("storage_backends"),
    )
    import nonebot

    nonebot.run()


if __name__ == "__main__":
    main()
