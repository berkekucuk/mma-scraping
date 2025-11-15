FROM python:3.12-slim

# Zaman dilimi için istersen:
ENV TZ=Europe/Istanbul

# Sistem paketleri: cron
RUN apt-get update && apt-get install -y cron && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Python bağımlılıkları
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Proje dosyalarını kopyala
COPY . .

# Log klasörünü oluştur
RUN mkdir -p /app/logs

# Cron job'ları yükle
# cronjobs dosyası user crontab formatında olduğu için direkt crontab'e yükleyeceğiz
RUN crontab /app/cronjobs

# cron'u foreground'da çalıştır (Docker container exit olmasın)
CMD ["cron", "-f"]
