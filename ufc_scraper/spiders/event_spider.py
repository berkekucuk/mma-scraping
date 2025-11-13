import scrapy
from ..utils.url_parser import UrlParser
from ..services.supabase_manager import SupabaseManager
from ..parsers.card_parser import CardParser


class EventSpider(scrapy.Spider):

    name = "event"
    allowed_domains = ["tapology.com"]

    custom_settings = {
        'CONCURRENT_REQUESTS': 1,
        'DOWNLOAD_DELAY': 2,
    }

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.supabase = SupabaseManager()

    async def start(self):
        url = "https://www.tapology.com/fightcenter/promotions/1-ultimate-fighting-championship-ufc?page=1"
        yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        events = response.css('div[data-controller="bout-toggler"]')

        self.logger.info(f"Found {len(events)} events on page")

        for event in events:
            event_relative_url = event.css("div.promotion a::attr(href)").get(default="")
            if not event_relative_url:
                continue

            event_name = event.css("div.promotion a::text").get(default="").strip()

            if event_name.startswith("Road to UFC"):
                self.logger.info(f"Skipping Road to UFC event: {event_name}")
                continue

            event_url = response.urljoin(event_relative_url)
            event_id = UrlParser.extract_event_id(event_relative_url)

            if not event_id:
                self.logger.error(f"Could not extract event_id from: {event_relative_url}")
                continue

            existing_event = self.supabase.get_event_by_id(event_id)

            if existing_event and self.is_event_complete(existing_event):
                self.logger.info(f"Event already exists with complete data: {event_id}")
                continue

            self.logger.info(f"Event needs scraping: {event_id}")

            yield scrapy.Request(
                url=event_url,
                callback=CardParser.parse_card,
                cb_kwargs={
                    "event_id": event_id,
                    "event_url": event_url,
                    "spider": self
                },
            )

    def is_event_complete(self, event):
        required_fields = ['name', 'status', 'datetime_utc', 'venue', 'location']
        return all(event.get(field) not in (None, "", " ", "N/A") for field in required_fields)
