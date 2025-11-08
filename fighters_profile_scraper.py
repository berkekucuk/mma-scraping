import os
import json
import time
import random
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter, Retry

# === AYARLAR ===
JSON_PATH = "/Users/berkekucuk/Documents/Scraping/ufc-scraping/data/fighters.json"
OUTPUT_DIR = "/Users/berkekucuk/Documents/Scraping/ufc-scraping/data/fighter_profiles"
ERROR_LOG = os.path.join(OUTPUT_DIR, "missing_profiles.txt")
PROGRESS_FILE = os.path.join(OUTPUT_DIR, "progress.txt")

MAX_WORKERS = 2              # Aynı anda kaç istek atılsın (2 önerilir)
BATCH_SIZE = 200             # Kaç fighter bir batch’te indirilsin
WAIT_BETWEEN_BATCHES = 90    # Batch’ler arası bekleme süresi (saniye)
TIMEOUT = 25

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)
HEADERS = {"User-Agent": USER_AGENT, "Referer": "https://www.tapology.com"}

# === KLASÖR OLUŞTUR ===
os.makedirs(OUTPUT_DIR, exist_ok=True)

# === SESSION: retry destekli ===
session = requests.Session()
retries = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504]
)
session.mount("https://", HTTPAdapter(max_retries=retries))

# === YARDIMCI FONKSİYONLAR ===
def log_error(fighter_id, url, reason):
    """Hatalı veya indirilemeyen sayfaları loglar."""
    with open(ERROR_LOG, "a", encoding="utf-8") as logf:
        logf.write(f"{fighter_id} | {url} | {reason}\n")

def save_progress(index):
    """Son işlenen batch index’ini kaydeder."""
    with open(PROGRESS_FILE, "w") as f:
        f.write(str(index))

def load_progress():
    """Son kaldığı batch index’ini okur."""
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r") as f:
            try:
                return int(f.read().strip())
            except:
                return 0
    return 0

# === PROFİL İNDİRME ===
def download_profile(fighter):
    """Tek bir dövüşçünün profil HTML sayfasını indirir."""
    fighter_id = fighter.get("fighter_id")
    profile_url = fighter.get("profile_url")

    if not fighter_id or not profile_url:
        log_error(fighter_id, profile_url, "Eksik veri")
        return f"⚠️  {fighter_id}: Eksik veri"

    safe_name = fighter_id.replace("/", "_")
    file_path = os.path.join(OUTPUT_DIR, f"{safe_name}.html")

    # Eğer dosya zaten varsa atla
    if os.path.exists(file_path):
        return f"⏭️  {fighter_id} zaten indirildi."

    try:
        response = session.get(profile_url, headers=HEADERS, timeout=TIMEOUT)
        if response.status_code != 200:
            log_error(fighter_id, profile_url, f"HTTP {response.status_code}")
            return f"❌  {fighter_id}: HTTP {response.status_code}"

        with open(file_path, "w", encoding="utf-8") as f:
            f.write(response.text)

        # Rastgele gecikme (bot gibi görünmemek için)
        time.sleep(random.uniform(1.2, 2.5))
        return f"✅  {fighter_id} indirildi."

    except Exception as e:
        log_error(fighter_id, profile_url, f"ERR: {str(e)}")
        return f"⚠️  {fighter_id}: {str(e)}"

# === BATCH İŞLEME ===
def process_batch(batch, batch_index):
    print(f"\n📦 {batch_index + 1}. batch başlıyor ({len(batch)} fighter)...\n")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(download_profile, fighter) for fighter in batch]
        for future in as_completed(futures):
            print(future.result())
    print(f"✅ {batch_index + 1}. batch tamamlandı!\n")

# === ANA FONKSİYON ===
def main():
    with open(JSON_PATH, "r", encoding="utf-8") as f:
        fighters = json.load(f)

    total = len(fighters)
    print(f"Toplam {total} fighter bulundu.\n")

    start_index = load_progress()
    print(f"📍 Kaldığı yerden devam ediliyor: {start_index}/{total}\n")

    if start_index >= total:
        print("✅ Tüm veriler zaten indirildi.")
        return

    for i in range(start_index, total, BATCH_SIZE):
        batch = fighters[i:i + BATCH_SIZE]
        batch_index = i // BATCH_SIZE

        process_batch(batch, batch_index)
        save_progress(i + BATCH_SIZE)  # kaydet

        if i + BATCH_SIZE < total:
            wait = random.uniform(WAIT_BETWEEN_BATCHES * 0.8, WAIT_BETWEEN_BATCHES * 1.2)
            print(f"⏸️  Sonraki batch’e geçmeden {int(wait)} saniye bekleniyor...\n")
            time.sleep(wait)

    print("\n🎯 Tüm fighter profilleri indirildi!")
    print(f"📁 HTML klasörü: {OUTPUT_DIR}")
    print(f"🧾 Hata logu: {ERROR_LOG}")
    print(f"📍 Progress kaydı: {PROGRESS_FILE}")

if __name__ == "__main__":
    main()
