FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    DRILL_DATA_DIR=/data \
    PORT=8000

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && groupadd --system --gid 10001 drill \
    && useradd --system --uid 10001 --gid 10001 --home-dir /app --shell /usr/sbin/nologin drill

COPY compressor.py app.py entrypoint.py ./
COPY templates ./templates
COPY static ./static

RUN mkdir -p /data && chown -R drill:drill /data /app

# Local imports must resolve from /app, not the (writable) cwd.
ENV PYTHONPATH=/app

EXPOSE 8000

# The entrypoint runs as root only to fix ownership of a freshly created data
# volume, then execs gunicorn as uid 10001.
# Single process, many threads: job state is in memory and the compression
# slots are OS threads, so --workers must stay at 1.
ENTRYPOINT ["python", "/app/entrypoint.py"]
CMD ["gunicorn", "-w", "1", "--threads", "16", "--timeout", "120", "--no-control-socket", "-b", "0.0.0.0:8000", "app:app"]
