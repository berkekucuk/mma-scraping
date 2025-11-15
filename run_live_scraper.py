import asyncio
import os
import sys
import subprocess
import logging
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s",
)
logger = logging.getLogger("live_scraper")

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = SCRIPT_DIR
sys.path.append(PROJECT_ROOT)

dotenv_path = os.path.join(PROJECT_ROOT, ".env")
load_dotenv(dotenv_path)

from ufc_scraper.services.supabase_manager import SupabaseManager


def get_scrapy_exec_path():
    python_exec = sys.executable
    bin_dir = os.path.dirname(python_exec)
    scrapy_path = os.path.join(bin_dir, "scrapy")

    if not os.path.exists(scrapy_path):
        logger.error(f"Scrapy bulunamadı: {scrapy_path}")
        return None

    return scrapy_path


async def main():
    logger.info("Checking for live events...")

    scrapy_exec = get_scrapy_exec_path()
    if scrapy_exec is None:
        return

    live_event = await SupabaseManager.get_live_event()

    if not live_event:
        logger.info("No live events.")
        return

    logger.info(f"Found a live event to scrape.")

    event_id = live_event.get("event_id")
    event_url = live_event.get("event_url")

    logger.info(f"Running smart spider for event {event_id}")

    command = [
        scrapy_exec,
        "crawl",
        "smart",
        "-a", f"event_id={event_id}",
        "-a", f"event_url={event_url}",
    ]

    subprocess.run(command, cwd=PROJECT_ROOT)


if __name__ == "__main__":
    asyncio.run(main())
