# syntax=docker/dockerfile:1
# L2 worker image. Consumes a queue and dispatches to registered L2 agents.
FROM python:3.12-slim AS builder
WORKDIR /build
RUN pip install --no-cache-dir build
COPY pyproject.toml README.md ./
COPY src ./src
RUN python -m build --wheel

FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
RUN useradd --create-home --uid 10001 maof
WORKDIR /app
COPY --from=builder /build/dist/*.whl /tmp/
RUN pip install "$(ls /tmp/*.whl)[all]" && rm -f /tmp/*.whl
COPY examples ./examples
USER maof
HEALTHCHECK --interval=30s --timeout=5s --retries=3 CMD python -c "import maof" || exit 1
# Adopters point --agents at the module that registers their L2 agents.
CMD ["maof", "run-worker", "--queue", "tasks.funds_commit", "--consumers", "examples/po_demo/consumers.yaml", "--agents", "examples.po_demo.register"]
