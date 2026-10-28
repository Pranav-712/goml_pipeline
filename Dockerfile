# Local, zero-infrastructure image: builds the warehouse and (optionally) runs the tests.
#   docker build -t medallion-pipeline .
#   docker run --rm -v "$(pwd)/out:/app/out" medallion-pipeline          # generate data + run pipeline
#   docker run --rm medallion-pipeline pytest -q                         # run the test-suite
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIPELINE_DATA_DIR=/app/out/data \
    PIPELINE_DB_PATH=/app/out/warehouse.duckdb

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src ./src
COPY sql ./sql
COPY tests ./tests
COPY pytest.ini .

# Non-root user; /app/out is where generated data, the DLQ, results and the DuckDB file land.
RUN useradd --create-home pipeline && mkdir -p /app/out && chown -R pipeline /app
USER pipeline
VOLUME ["/app/out"]

CMD ["python", "-m", "src.pipeline", "--generate", "--log-format", "json"]
