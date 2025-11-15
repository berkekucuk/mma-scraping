import scrapy
from ..utils.url_parser import UrlParser
from ..services.supabase_manager import SupabaseManager
from ..parsers.event_page_parser import EventPageParser

class SmartSpider(scrapy.Spider):

    name = "smart"
    allowed_domains = ["tapology.com"]

    def __init__(self, *args, **kwargs):
        super(SmartSpider, self).__init__(*args, **kwargs)
        self.supabase = SupabaseManager

        # Parameters for live mode
        self.event_id = kwargs.get('event_id')
        self.event_url = kwargs.get('event_url')
        # ==========================================

    async def start(self):
        if self.event_id and self.event_url:
            # (LIVE) MOD
            # trigger by run_live_scraper.py
            self.logger.info(f"[LIVE MODE] Scraping single event: {self.event_id}")
            yield scrapy.Request(
                url=self.event_url,
                callback=self.parse_live_event,
                cb_kwargs={"event_id": self.event_id, "event_url": self.event_url}
            )

        else:
            # MANUEL MOD
            # trigger by 'scrapy crawl event' command
            self.logger.info("[FULL MODE] Starting full event pagination scrape...")
            url = "https://www.tapology.com/fightcenter/promotions/1-ultimate-fighting-championship-ufc?page=1"
            yield scrapy.Request(url, callback=self.parse_uncompleted_events)
        # ==================================

    def parse_live_event(self, response, event_id, event_url):

        self.logger.debug(f"Parsing event status for {event_id}")
        yield from EventPageParser.parse_card(response, event_id, event_url)


    async def parse_uncompleted_events(self, response):

        events = response.css('div[data-controller="bout-toggler"]')
        self.logger.info(f"Found {len(events)} events on page {response.url}")

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

            existing_event = await self.supabase.get_event_by_id(event_id)

            if not existing_event or existing_event.get("status") != "Completed":

                self.logger.info(f"Event {event_id} is new or not completed. Scheduling full page scrape.")

                yield scrapy.Request(
                    url=event_url,
                    callback=EventPageParser.parse_card,
                    cb_kwargs={"event_id": event_id, "event_url": event_url},
                )
            else:
                self.logger.debug(f"Event {event_id} is already completed. Skipping.")

