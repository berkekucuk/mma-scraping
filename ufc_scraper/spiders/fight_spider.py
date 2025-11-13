import scrapy
from ..parsers.fight_parser import FightParser
from ..services.supabase_manager import SupabaseManager


class FightSpider(scrapy.Spider):

    name = "ufc_fights"
    allowed_domains = ["tapology.com"]

    custom_settings = {
        'CONCURRENT_REQUESTS': 1,
        'DOWNLOAD_DELAY': 2,
    }

    def __init__(self, *args, **kwargs):
        super(FightSpider, self).__init__(*args, **kwargs)
        self.supabase = SupabaseManager()

    def start_requests(self):
        # Supabase'den upcoming eventleri al
        upcoming_events = self.supabase.get_upcoming_events()

        if not upcoming_events:
            self.logger.warning("No upcoming events found. Stopping.")
            return

        for event in upcoming_events:
            event_url = event.get('event_url')
            event_id = event.get('event_id')

            if event_url and event_id:
                self.logger.info(f"Scraping fights for event: {event_id}")
                yield scrapy.Request(url=event_url, callback=FightParser.parse_fights, cb_kwargs={"event_id": event_id})

    def is_fighter_complete(self, fighter):
        core_fields = ['date_of_birth', 'height', 'reach', 'born']

        # En az bir tanesi doluysa → complete
        for field in core_fields:
            value = fighter.get(field)
            if value not in (None, "", "N/A", "-", "—"):
                return True

        return False
