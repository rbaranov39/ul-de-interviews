FROM apache/superset:6.1.0

USER root
COPY prepare_assets.py /app/prepare_assets.py
RUN . /app/.venv/bin/activate \
    && uv pip install --no-cache \
        "psycopg2-binary==2.9.12" \
        "clickhouse-connect>=1.8.0,<2.0"
USER superset
