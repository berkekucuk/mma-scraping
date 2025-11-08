import scrapy
import json
import os
from scrapy.http import HtmlResponse
from ..items import FighterItem
from ..parsers.fighter_parser import FighterParser


class FighterSpider(scrapy.Spider):
    name = "fighter"
    allowed_domains = ["tapology.com"]

    def start_requests(self):
        with open("data/fighters.json", "r", encoding="utf-8") as f:
            fighters = json.load(f)

        for fighter in fighters:
            url = fighter.get("profile_url")
            fighter_id = fighter.get("fighter_id")
            if url and fighter_id:
                html_path = f"fighter_profiles/{fighter_id}.html"
                if os.path.exists(html_path):
                    with open(html_path, "r", encoding="utf-8") as f:
                        html_content = f.read()

                    response = HtmlResponse(
                        url=url,
                        body=html_content,
                        encoding='utf-8'
                    )

                    for item in self.parse_fighter(response, fighter_id):
                        yield item

    def parse_fighter(self, response, fighter_id):
        fighter_data = FighterParser.parse_fighter(response)
        fighter_data["fighter_id"] = fighter_id
        yield FighterItem(**fighter_data)
