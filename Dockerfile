FROM python:3.11-slim
WORKDIR /app
RUN useradd -m app && chown -R app /app
USER app
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=3s CMD python -c "print('ok')"
CMD ["python","-c","print('controller stub up', flush=True); import time; time.sleep(10**9)"]
