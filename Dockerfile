FROM python:3.12-slim

RUN apt-get update && apt-get install -y cron && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /app/logs

# Boş log dosyaları oluştur
RUN touch /app/logs/uncompleted_events.log /app/logs/live_events.log

RUN crontab /app/cronjobs

CMD cron && tail -f /app/logs/live_events.log
