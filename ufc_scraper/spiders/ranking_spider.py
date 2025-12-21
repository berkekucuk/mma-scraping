import os
import logging
from datetime import datetime
import scrapy
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()


class RankingSpider(scrapy.Spider):
    name = "ranking"
    start_urls = ['https://www.ufc.com/rankings']

    WEIGHT_CLASS_MAPPING = {
        "Men's Pound-for-Pound Top Rank": "mens_p4p",
        "Men's Pound-for-Pound": "mens_p4p",
        "Flyweight": "FLW",
        "Bantamweight": "BW",
        "Featherweight": "FW",
        "Lightweight": "LW",
        "Welterweight": "WW",
        "Middleweight": "MW",
        "Light Heavyweight": "LHW",
        "Heavyweight": "HW",
        "Women's Pound-for-Pound Top Rank": "womens_p4p",
        "Women's Pound-for-Pound": "womens_p4p",
        "Women's Strawweight": "SW",
        "Women's Flyweight": "W_FLW",
        "Women's Bantamweight": "W_BW",
        "Women's Featherweight": "W_FW"
    }

    NAME_EXCEPTIONS = {
        "Loopy Godinez": "Lupita Godinez",
        "Benoît Saint Denis": "Benoit Saint-Denis",
        "Benoit Saint Denis": "Benoit Saint-Denis",
        "Zhang Weili": "Weili Zhang",
        "Song Yadong": "Yadong Song",
        "Yan Xiaonan": "Xiaonan Yan",
        "Ailin Perez": "Ailín Pérez",
        "Waldo Cortes Acosta": "Waldo Cortes-Acosta",
        "Natalia Silva": "Natália Silva",
        "Lone’er Kavanagh": "Lone'er Kavanagh",
        "Farès Ziam": "Fares Ziam",
        "Zhang Mingyang": "Mingyang Zhang",
        "Wang Cong": "Cong Wang"
    }

    def __init__(self, *args, **kwargs):
        super(RankingSpider, self).__init__(*args, **kwargs)
        self._setup_file_logger()

        supabase_url = os.getenv("SUPABASE_URL")
        supabase_key = os.getenv("SUPABASE_KEY")

        if not supabase_url or not supabase_key:
            raise ValueError("SUPABASE_URL and SUPABASE_KEY environment variables are required")

        self.supabase: Client = create_client(supabase_url, supabase_key)
        self._load_fighter_cache()

    def _load_fighter_cache(self):
        self.fighter_cache = {}
        batch_size = 1000
        offset = 0

        while True:
            response = self.supabase.table('fighters').select('fighter_id, name').range(offset, offset + batch_size - 1).execute()

            if not response.data:
                break

            for f in response.data:
                self.fighter_cache[f['name'].strip()] = f['fighter_id']

            if len(response.data) < batch_size:
                break

            offset += batch_size

    def _setup_file_logger(self):
        logs_dir = os.path.join(os.getcwd(), "logs")
        os.makedirs(logs_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = os.path.join(logs_dir, f"ranking_errors_{timestamp}.txt")

        self.file_logger = logging.getLogger(f"ranking_file_logger_{timestamp}")
        self.file_logger.setLevel(logging.WARNING)
        self.file_logger.handlers.clear()

        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
        self.file_logger.addHandler(file_handler)
        self.log_file_path = log_file

    def parse(self, response):
        groupings = response.css('.view-grouping')

        for group in groupings:
            raw_title = group.css('.view-grouping-header::text').get()
            if not raw_title:
                continue

            title = raw_title.strip()
            db_weight_class_id = self.WEIGHT_CLASS_MAPPING.get(title)

            if not db_weight_class_id:
                self.file_logger.warning(f"Weight class not matched: {title}")
                continue

            champion_name = group.css('.info h5 a::text').get()
            if champion_name:
                self.process_fighter(champion_name, db_weight_class_id, 0)

            rows = group.css('tbody tr')
            current_rank = 1

            for row in rows:
                fighter_name = row.css('.views-field-title a::text').get()

                if fighter_name:
                    self.process_fighter(fighter_name, db_weight_class_id, current_rank)
                    current_rank += 1

    def process_fighter(self, fighter_name, weight_class_id, rank):
        fighter_name = fighter_name.strip()

        search_name = self.NAME_EXCEPTIONS.get(fighter_name, fighter_name)
        found_id = self.fighter_cache.get(search_name)

        if found_id:
            data = {
                "weight_class_id": weight_class_id,
                "fighter_id": found_id,
                "rank_number": rank,
                "updated_at": "now()"
            }
            try:
                self.supabase.table('rankings').upsert(data, on_conflict='weight_class_id, rank_number').execute()
                return True
            except Exception as e:
                self.file_logger.error(f"Write error - {fighter_name}: {e}")
                return False
        else:
            self.file_logger.warning(f"Fighter not found in DB: {fighter_name}")
            return False
