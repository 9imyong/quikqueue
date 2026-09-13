FROM python:3.11-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1
COPY requirements.txt .
RUN pip install -U pip && pip install --no-cache-dir -r requirements.txt
COPY worker_app ./worker_app
RUN useradd --create-home --uid 10001 app && chown -R app /app
USER app
CMD ["celery", "-A", "worker_app.celery_app", "worker", "-l", "info", "-Q", "celery"]
