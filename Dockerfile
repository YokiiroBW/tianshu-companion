# Companion runtime image: one process, one SQLite owner, no baked credentials.
#
# The image is deliberately thin. It installs the exact runtime pins, copies the package, and
# declares the deployment paths the runtime CLI already defaults to on Linux. It does not
# download a model, does not bake a token, does not carry the test suite or the coordination
# repository, and does not run as root.
#
# What is *not* claimed: this file has not been built in this batch. See docs/deployment.md -
# "a Dockerfile is not a build", and an unbuilt image is an unverified image.

FROM python:3.12-slim

# The runtime pins, spelled out here so the image's dependency set is visible in the image
# itself and cannot drift with a resolver decision made at build time.
RUN pip install --no-cache-dir \
    "fastapi==0.135.1" \
    "uvicorn==0.42.0" \
    "httpx==0.28.1" \
    "jsonschema==4.26.0"

WORKDIR /app

# Only what the process needs at run time: the package, and the entry point it launches.
COPY pyproject.toml README.md /app/
COPY src /app/src
COPY integrations /app/integrations
COPY scripts/container_healthcheck.py /app/scripts/container_healthcheck.py

# `--no-deps` because the pins above are the dependency set; the resolver must not silently
# upgrade one of them here.
RUN pip install --no-cache-dir --no-deps -e /app

# The deployment defaults, matching runtime_cli.CONTAINER_PATHS. Every one of them is a mount
# point or an explicit path, so a container started without flags is still explicit.
ENV TIANSHU_COMPANION_CONFIG=/config/companion.json \
    TIANSHU_CONTRACTS=/contracts \
    TIANSHU_COMPANION_DATABASE=/data/companion.db \
    TIANSHU_LOG_DIR=/var/log/tianshu \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# One non-root identity, fixed, so a mounted volume's ownership is predictable.
RUN useradd --uid 10001 --create-home --shell /usr/sbin/nologin companion \
    && mkdir -p /config /contracts /data /var/log/tianshu \
    && chown -R 10001:10001 /data /var/log/tianshu

USER 10001:10001

# 8765 is the documented default; it is not published here, because publishing is a deployment
# decision and the bind address inside the network namespace is the operator's.
EXPOSE 8765

# Liveness only. Readiness is authenticated and deliberately not what a restart policy uses;
# see scripts/container_healthcheck.py for why.
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD ["python", "/app/scripts/container_healthcheck.py"]

# Exec form, so the signal reaches the runtime and its own handler performs the orderly
# shutdown that releases the single-owner lock. One worker, fixed by the CLI.
ENTRYPOINT ["python", "-m", "tianshu_companion.runtime_cli"]
