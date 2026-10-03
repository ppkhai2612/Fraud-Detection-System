FROM python:3.11-slim AS builder

RUN pip install --no-cache-dir uv

WORKDIR /app

COPY generator/pyproject.toml .
RUN uv venv .venv && uv sync --no-dev

COPY generator/src/ src/

FROM python:3.11-slim

WORKDIR /app

# Schemas from project root (build context is project root)
COPY schemas/ schemas/

ENV PATH="/app/.venv/bin:$PATH"

CMD [ "python", "-m", "src.transaction_generator" ]