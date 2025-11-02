import scrapy
from ..services.html_cache_manager import HtmlCacheManager
from ..utils.url_util import URLUtil
from ..parsers.card_parser import CardParser
from ..parsers.fight_parser import FightParser
from ..items import EventItem

class UFCEventsSpider(scrapy.Spider):

    name = "ufc_events"
    allowed_domains = ["tapology.com"]

    def start_requests(self):
        url = "https://www.tapology.com/fightcenter/promotions/1-ultimate-fighting-championship-ufc?page=2"
        for item in self.fetch_or_load(url=url, callback=self.parse):
            yield item

    # def start_requests(self):
    #     base_url = "https://www.tapology.com/fightcenter/promotions/1-ultimate-fighting-championship-ufc?page="

    #     for page_num in range(2, 32):  # 2’den 31’e kadar (31 dahil)
    #         url = f"{base_url}{page_num}"
    #         for item in self.fetch_or_load(url=url, callback=self.parse):
    #             yield item

    # cb_kwargs(callback keyword arguments) Callback fonksiyonuna gönderilecek ek bilgiler (varsayılan: None)
    def fetch_or_load(self, url, callback, cb_kwargs=None):
        response = HtmlCacheManager.load_from_cache(url)
        if response is not None:
            for item in callback(response, **(cb_kwargs or {})):
                yield item
        else:
            yield scrapy.Request(
                url=url,
                callback=self.save_and_parse,
                cb_kwargs={
                    'original_callback': callback,
                    'url': url,
                    'cb_kwargs': cb_kwargs or {}
                }
            )

    def save_and_parse(self, response, original_callback, url, cb_kwargs):
        HtmlCacheManager.save_to_cache(url, response)
        for item in original_callback(response, **(cb_kwargs or {})):
            yield item

    def parse(self, response):
        events = response.css('div.flex.flex-col.border-b.border-solid.border-neutral-700')
        for event in events:
            event_relative_url = event.css('div.promotion a::attr(href)').get(default='')
            if event_relative_url is not None:
                event_url = response.urljoin(event_relative_url)
                event_id = URLUtil.extract_event_id(event_relative_url)
                for item in self.fetch_or_load(url=event_url, callback=self.parse_event, cb_kwargs={'event_id': event_id, 'event_url': event_url}):
                    yield item

    def parse_event(self, response, event_id, event_url):
        card_data = CardParser.parse_card(response)

        # --- Event verisi ---
        event_item = EventItem()
        event_item['event_id'] = event_id
        event_item['event_url'] = event_url
        event_item.update(card_data)
        yield event_item

        # --- Fight verilerini parse et ---
        for fight_data in FightParser.parse_fights(response, event_id):
            yield fight_data  # FightItem, FighterItem ve FightParticipationItem ayrı ayrı yield edilir
