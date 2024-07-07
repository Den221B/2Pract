FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir telebot requests psycopg2-binary

WORKDIR /app

COPY main.py .

CMD ["python", "main.py"]