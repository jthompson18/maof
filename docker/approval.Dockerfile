# syntax=docker/dockerfile:1
# HITL approval service image (FastAPI).
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
USER maof
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s --retries=3 CMD python -c "import maof" || exit 1
CMD ["maof", "run-approval"]
