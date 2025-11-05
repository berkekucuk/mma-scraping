import scrapy
import json
from ..items import FighterItem
from ..parsers.fighter_parser import FighterParser
from ..services.html_cache_manager import HtmlCacheManager


class FighterSpider(scrapy.Spider):
    name = "fighter"
    allowed_domains = ["tapology.com"]

    def start_requests(self):
        # Fighter linklerini fighters.json'dan oku
        # with open("data/json_output/fighters.json", "r", encoding="utf-8") as f:
        #     fighters = json.load(f)

        # for fighter in fighters:
        #     url = fighter.get("profile_url")
        #     fighter_id = fighter.get("fighter_id")
        #     if url is not None:
        #         for item in self.fetch_or_load(url, self.parse_fighter, cb_kwargs={"fighter_id": fighter_id}):
        #             yield item
        test_url = "https://www.tapology.com/fightcenter/fighters/119831-jack-della-maddalena"
        fighter_id = "119831"  # veya ID sayısal ise "1425" gibi

        yield from self.fetch_or_load(
            url=test_url,
            callback=self.parse_fighter,
            cb_kwargs={"fighter_id": fighter_id},
        )

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
                    "original_callback": callback,
                    "url": url,
                    "cb_kwargs": cb_kwargs or {},
                },
            )

    def save_and_parse(self, response, original_callback, url, cb_kwargs):
        HtmlCacheManager.save_to_cache(url, response)
        for item in original_callback(response, **(cb_kwargs or {})):
            yield item

    def parse_fighter(self, response, fighter_id):
        fighter_data = FighterParser.parse_fighter(response)
        fighter_data["fighter_id"] = fighter_id
        yield FighterItem(**fighter_data)
