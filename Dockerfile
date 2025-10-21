FROM python:3.11-slim
WORKDIR /app
RUN useradd -m app && chown -R app /app
USER app
EXPOSE 8080
HEALTHCHECK CMD python - <<'PY' || exit 1
print("ok")
PY
CMD python - <<'PY'
print("controller stub up", flush=True)
import time; time.sleep(10**9)
PY
