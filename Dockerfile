FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Minimal system deps (curl for healthcheck)
RUN apt-get update && \
    apt-get install -y curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Copy controller code
COPY . /app

# Healthcheck (same as you had)
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
  CMD curl -fsS http://0.0.0.0:8080/healthz || exit 1

# Single-process, async, uvloop, no split-brain
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080", "--loop", "uvloop", "--workers", "1"]
