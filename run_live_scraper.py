import asyncio
import os
import sys
import subprocess
from dotenv import load_dotenv

# --- 1. Proje Yollarını Otomatik Olarak Bul ---
# Bu script'in (run_live_scraper.py) bulunduğu dizini al
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# Proje ana dizini (bu script'in olduğu yer)
PROJECT_ROOT = SCRIPT_DIR

# Python'un 'ufc_scraper' modülünü bulabilmesi için
# proje dizinini sistem yoluna (sys.path) ekle
sys.path.append(PROJECT_ROOT)

# --- 2. Environment (.env) Dosyasını Yükle ---
# .env dosyasının tam yolunu belirle
dotenv_path = os.path.join(PROJECT_ROOT, ".env")
load_dotenv(dotenv_path)

# Path'i ve .env'i yükledikten sonra artık modüllerimizi import edebiliriz
from ufc_scraper.services.supabase_manager import SupabaseManager

async def main():
    """
    Her dakika çalışır, canlı (ve status'u henüz 'Completed' olmayan)
    etkinlik var mı diye kontrol eder ve 'event' spider'ını
    'canlı mod'da tetikler.
    """
    print(f"[{asyncio.get_event_loop().time()}] Checking for live events...")

    # --- 3. Scrapy Executable'ını Dinamik Olarak Bul ---

    # Bu script'i çalıştıran Python'un tam yolunu al (örn: .../.venv/bin/python)
    python_exec_path = sys.executable
    # 'bin' klasörünün yolunu bul (örn: .../.venv/bin)
    bin_dir = os.path.dirname(python_exec_path)
    # 'scrapy' komutunun tam yolunu oluştur (örn: .../.venv/bin/scrapy)
    scrapy_exec_path = os.path.join(bin_dir, "scrapy")

    if not os.path.exists(scrapy_exec_path):
        print(f"HATA: Scrapy komutu bulunamadı: {scrapy_exec_path}")
        return

    # --- 4. Supabase'i Kontrol Et ---
    supabase = SupabaseManager
    # 'get_live_events_to_scrape' (status != 'Completed' olanları bulan)
    live_events = await supabase.get_live_events()

    if not live_events:
        print("No live events to scrape. Exiting.")
        return

    print(f"Found {len(live_events)} live event(s).")

    for event in live_events:
        event_id = event.get('event_id')
        event_url = event.get('event_url')

        if not event_id or not event_url:
            continue

        print(f"Triggering 'smart' spider in LIVE MODE for event: {event_id}...")

        # --- 5. event SPIDER'ını 'Canlı Mod'da Çalıştır ---
        # Spider'ınızın adını "smart" olarak değiştirdiğinizi varsayıyorum
        command = [
            scrapy_exec_path,
            "crawl",
            "smart", # 'event' spider'ınızın yeni adı
            "-a", f"event_id={event_id}",
            "-a", f"event_url={event_url}"
        ]

        # Komutu projenin ana dizininden (cwd) çalıştır
        subprocess.run(command, cwd=PROJECT_ROOT)

if __name__ == "__main__":
    asyncio.run(main())
