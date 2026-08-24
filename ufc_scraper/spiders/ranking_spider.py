import scrapy
from ..services.supabase_manager import SupabaseManager
from ..parsers.ranking_parser import parse_rankings


class RankingSpider(scrapy.Spider):
    name = "ranking"
    allowed_domains = ['ufc.com']

    def __init__(self, *args, **kwargs):
        super(RankingSpider, self).__init__(*args, **kwargs)
        self.supabase = SupabaseManager()
        self.fighter_cache = {}

    async def start(self):
        self.fighter_cache = await self.supabase.load_fighter_cache()

        if not self.fighter_cache:
            self.logger.error("⚠️ Fighter Cache is empty! Rankings might not link correctly.")

        yield scrapy.Request(
            url='https://www.ufc.com/rankings',
            callback=parse_rankings,
            cb_kwargs={"fighter_cache": self.fighter_cache}
        )
