import scrapy
from ..parsers.fight_parser import FightParser
from ..services.supabase_manager import SupabaseManager


class FightSpider(scrapy.Spider):

    name = "fight"
    allowed_domains = ["tapology.com"]

    def __init__(self, *args, **kwargs):
        super(FightSpider, self).__init__(*args, **kwargs)
        self.supabase = SupabaseManager

    async def start(self):
        # Supabase'den upcoming eventleri al
        upcoming_events = await self.supabase.get_upcoming_events()

        if not upcoming_events:
            self.logger.warning("No upcoming events found. Stopping.")
            return

        for event in upcoming_events:
            event_url = event.get('event_url')
            event_id = event.get('event_id')

            if event_url and event_id:
                self.logger.info(f"Scraping fights for event: {event_id}")
                yield scrapy.Request(
                    url=event_url,
                    callback=FightParser.parse_fights,
                    cb_kwargs={"event_id": event_id}
                    )

