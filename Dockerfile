FROM python:3.11-slim

RUN pip install telebot requests psycopg2-binary

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       postgresql-client \
    && rm -rf /var/lib/apt/lists/*

COPY main.py /app/main.py

WORKDIR /app

RUN pip install --no-cache-dir requests psycopg2-binary

ENV DATABASE_URL=postgresql://user:password@db:5432/vacancies_db

CMD ["python", "main.py"]