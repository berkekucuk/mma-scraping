# 1. Adım: Temel İmaj (Base Image)
# 'slim' versiyonu, production için daha küçük ve güvenlidir.
FROM python:3.11-slim

# 2. Adım: Sistem Bağımlılıklarını Kur
# cron -> Zamanlayıcı (crontab) için
# postgresql-client -> pg_dump, psql gibi komutlar için (yedekleme vb.)
RUN apt-get update && apt-get install -y \
    cron \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# 3. Adım: Çalışma Dizinini Ayarla
# Koddaki tüm yolların (örn: '/app/logs') çalışması için
WORKDIR /app

# 4. Adım: Python Bağımlılıklarını Kur
# Önce SADECE requirements.txt'yi kopyala (Docker önbelleği için en verimli yöntem)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 5. Adım: Proje Kodunu Kopyala
# Kalan tüm proje dosyalarını (/app dizinine) kopyala
# (.dockerignore sayesinde .env, .git, venv gibi dosyalar atlanacak)
COPY . .

# 6. Adım: Cron (Zamanlayıcı) Kurulumu
# Proje dizinimizdeki 'scraper.crontab' dosyasını,
# cron'un okuyacağı '/etc/cron.d/' dizinine kopyala
COPY scraper.crontab /etc/cron.d/scraper-cron

# Crontab dosyasına doğru dosya izinlerini ver
RUN chmod 0644 /etc/cron.d/scraper-cron

# 7. Adım: Log Dosyalarını ve Dizinini Oluştur
# Cron'un log yazabilmesi için 'logs' dizinini ve boş log dosyalarını oluştur
RUN mkdir -p /app/logs
RUN touch /app/logs/cron.log
RUN touch /app/logs/full_scrape.log

# 8. Adım: Konteyner Başlangıç Komutu
# Konteyner başladığında cron servisini başlat
# ve 'tail -f' komutu ile log dosyasını takip ederek konteynerin kapanmasını engelle
CMD cron && tail -f /app/logs/cron.log
