import scrapy
from ..utils.url_parser import UrlParser
from ..parsers.card_parser import CardParser
from ..services.supabase_manager import SupabaseManager

class EventSpider(scrapy.Spider):

    name = "event"
    allowed_domains = ["tapology.com"]

    def __init__(self, *args, **kwargs):
        super(EventSpider, self).__init__(*args, **kwargs)
        self.supabase = SupabaseManager

    async def start(self):
        url = "https://www.tapology.com/fightcenter/promotions/1-ultimate-fighting-championship-ufc?page=1"
        yield scrapy.Request(url, callback=self.parse)

    # ❗ PARSE METODU ASYNC OLARAK DEĞİŞTİ
    async def parse(self, response):
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

            # ----------------------------------------------------
            # ❗ KONTROL MANTIĞI BURAYA ASENKRON OLARAK EKLENDİ
            # ----------------------------------------------------
            current_event = await self.supabase.get_event_by_id(event_id)

            if current_event and self.is_event_complete(current_event):
                self.logger.debug(f"Event already exists with complete data: {event_id}. Skipping request.")
                continue  # <-- Bu etkinlik tam, detay sayfasına GİTME
            # ----------------------------------------------------

            if not current_event:
                self.logger.info(f"New event found. Yielding request: {event_id}")
            else:
                self.logger.info(f"Incomplete event data. Yielding request: {event_id}")

            yield scrapy.Request(
                url=event_url,
                callback=CardParser.parse_card,
                cb_kwargs={
                    "event_id": event_id,
                    "event_url": event_url,
                },
            )

    # ❗ KONTROL METODU GERİ EKLENDİ
    def is_event_complete(self, event: dict) -> bool:
        required_fields = ['name', 'status', 'datetime_utc', 'venue', 'location']

        # 'N/A', 'TBD' gibi eksik verileri de kontrol et
        incomplete_values = (None, "", " ", "N/A", "TBD")

        for field in required_fields:
            if event.get(field) in incomplete_values:
                return False
        return True
