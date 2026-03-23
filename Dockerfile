# aillium-UI-TARS — Transitional worker (polling executor)
# Runs the polling loop: poll → claim → execute → callback

FROM python:3.12-slim AS runtime
WORKDIR /app

# Install git for git+https pip dependencies (aillium-schemas)
RUN apt-get update && apt-get install -y --no-install-recommends git && \
    rm -rf /var/lib/apt/lists/*

COPY codes/pyproject.toml codes/uv.lock* ./
RUN pip install --no-cache-dir .

COPY codes/ ./

# Default environment — override in compose or deployment config.
ENV AILLIUM_POLL_INTERVAL_SECONDS=0.5
ENV AILLIUM_IDLE_BACKOFF_SECONDS=2.0
ENV AILLIUM_EXECUTOR_TYPE=ui-tars
ENV AILLIUM_TASK_TYPE=remote-handshake
ENV MESHCENTRAL_MOCK=1

CMD ["python", "-m", "ui_tars.executor.worker"]
