import scrapy
from ..utils.url_parser import UrlParser
from ..services.supabase_manager import SupabaseManager
from ..parsers.event_page_parser import EventPageParser


class SmartSpider(scrapy.Spider):

    name = "smart"
    allowed_domains = ["tapology.com"]

    def __init__(self, *args, **kwargs):
        super(SmartSpider, self).__init__(*args, **kwargs)
        self.supabase = SupabaseManager()
        self.mode = kwargs.get('mode', 'upcoming')  # 'live' or 'upcoming'


    async def start(self):
        if self.mode == 'live':
            live_event = await self.supabase.get_live_event()

            if not live_event:
                self.logger.info("[LIVE MODE] No live events found. Exiting.")
                return

            event_id = live_event.get("event_id")
            event_url = live_event.get("event_url")

            self.logger.info(f"[LIVE MODE] Scraping event: {event_id}")
            yield scrapy.Request(
                url=event_url,
                callback=self.parse_live_event,
                cb_kwargs={"event_id": event_id, "event_url": event_url}
            )

        else:
            self.logger.info("[UPCOMING MODE] Starting event pagination scrape...")
            url = "https://www.tapology.com/fightcenter/promotions/1-ultimate-fighting-championship-ufc?page=1"
            yield scrapy.Request(
                url=url,
                callback=self.parse_upcoming_events
                )


    def parse_live_event(self, response, event_id, event_url):

        self.logger.debug(f"Parsing event status for {event_id}")
        yield from EventPageParser.parse_card(response, event_id, event_url, is_live_mode=True)


    async def parse_upcoming_events(self, response):

        events = response.css('div[data-controller="bout-toggler"]')
        self.logger.info(f"Found {len(events)} events on page {response.url}")

        event_data_list = []
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

            event_data_list.append({"event_id": event_id, "event_url": event_url})

        if event_data_list:
            event_ids = [item["event_id"] for item in event_data_list]
            existing_events = await self.supabase.get_events_by_ids(event_ids)

            for event_data in event_data_list:
                event_id = event_data["event_id"]
                event_url = event_data["event_url"]

                existing_event = existing_events.get(event_id)

                if not existing_event or existing_event.get("status") == "Upcoming":
                    self.logger.info(f"Event {event_id} is new or upcoming. Scheduling full page scrape.")

                    yield scrapy.Request(
                        url=event_url,
                        callback=EventPageParser.parse_card,
                        cb_kwargs={"event_id": event_id, "event_url": event_url},
                    )
                else:
                    self.logger.debug(f"Event {event_id} is already completed. Skipping.")
