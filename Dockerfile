FROM python:3.12-slim AS base
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
# Default: full crawl; override CMD for seed or dev
CMD ["scrapy", "crawl", "vieclam", "-s", "LOG_LEVEL=INFO"]
