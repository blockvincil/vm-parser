FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
RUN apt-get update && apt-get install -y --no-install-recommends librdkafka-dev gcc \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /srv
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app ./app
COPY config ./config
COPY sql ./sql
RUN useradd -m app && chown -R app /srv
USER app
CMD ["python", "-m", "app.worker"]
