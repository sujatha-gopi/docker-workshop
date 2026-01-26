FROM python:3.13.11

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

WORKDIR /app

ENV PATH="/app/.venv/bin:$PATH"

COPY "pyproject.toml" ".python-version" "uv.lock" ./

COPY greentaxitrip.py greentaxitrip.py

RUN uv sync --locked

ENTRYPOINT ["python", "greentaxitrip.py"]