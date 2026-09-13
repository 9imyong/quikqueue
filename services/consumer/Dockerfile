FROM python:3.11-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1
COPY requirements.txt .
RUN pip install -U pip && pip install --no-cache-dir -r requirements.txt
COPY run_consumer.py .
RUN useradd --create-home --uid 10001 app && chown -R app /app
USER app
CMD ["python", "run_consumer.py"]
