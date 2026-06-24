# syntax=docker/dockerfile:1
# L1 orchestrator image. Runs the adopter's orchestrator workload (here, the po_demo).
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
# Adopters replace this with their own orchestrator entrypoint. The compose stack
# overrides this with main_distributed (broker-connected); the default runs the
# fully in-memory demo.
CMD ["python", "-m", "examples.po_demo.main"]
