import scrapy

from ..services.supabase_manager import SupabaseManager
from ..utils.date_parser import DateParser
from ..utils.record_parser import RecordParser
from ..utils.url_parser import UrlParser
from ..utils.measurement_parser import MeasurementParser
from ..utils.weight_class_mapper import WeightClassMapper

class FighterSpider(scrapy.Spider):
    name = "fighter"
    allowed_domains = ["tapology.com"]

    def __init__(self, *args, **kwargs):
        super(FighterSpider, self).__init__(*args, **kwargs)
        self.fighter_id = kwargs.get('fighter_id')
        self.target_url = kwargs.get('profile_url')

    async def start(self):
        if self.target_url and self.fighter_id:
            self.logger.info(f"[RESCUE MODE] Starting scrape for Fighter ID: {self.fighter_id}")

            yield scrapy.Request(
                url=self.target_url,
                callback=self.parse,
                cb_kwargs={
                    'fighter_id': self.fighter_id,
                    'profile_url': self.target_url
                },
                dont_filter=True
            )
        else:
            self.logger.error("Missing required arguments! Usage: scrapy crawl fighter -a url=... -a fighter_id=...")

    async def parse(self, response, fighter_id, profile_url):
        header = response.css("div#fighterPageHeader")
        container = response.css("div#standardDetails")

        nickname = self._extract_detail(container, "Nickname:")

        record_str = self._extract_detail(container, "Pro MMA Record:")
        record = RecordParser.parse(record_str)

        date_of_birth_str = self._extract_detail(container, "Date of Birth:")
        date_of_birth = DateParser.parse_date_to_iso(date_of_birth_str)

        height_str = self._extract_detail(container, "Height:")
        height = MeasurementParser.parse_measurement(height_str)

        reach_str = self._extract_reach(container)
        reach = MeasurementParser.parse_measurement(reach_str)

        weight_class_name = self._extract_detail(container, "Weight Class:")
        weight_class_id = WeightClassMapper.map_weight_class(weight_class_name)

        born = self._extract_detail(container, "Born:")
        fighting_out_of = self._extract_detail(container, "Fighting out of:")
        style = self._extract_detail(container, "Foundation Style:")

        country_flag_relative_url = header.css("img::attr(src)").get(default="").strip() or None
        country_flag_url = response.urljoin(country_flag_relative_url) if country_flag_relative_url else None
        country_code = UrlParser.extract_country_code(country_flag_url) if country_flag_url else None

        update_data = {
            "nickname": nickname,
            "record": record,
            "date_of_birth": date_of_birth,
            "height": height,
            "reach": reach,
            "weight_class_id": weight_class_id,
            "born": born,
            "fighting_out_of": fighting_out_of,
            "style": style,
            "country_code": country_code,
        }

        self.logger.info(f"[DIRECT UPDATE] Updating DB for fighter: {fighter_id}")

        try:
            await SupabaseManager.update_fighter(fighter_id, update_data)
            self.logger.info(f"[SUCCESS] Fighter {fighter_id} updated successfully.")
        except Exception as e:
            self.logger.error(f"[ERROR] Failed to update fighter {fighter_id} inside Spider: {e}")

        return

    def _extract_detail(self, container, label):
        val = container.xpath(f'.//strong[contains(text(), "{label}")]/following-sibling::span[1]/text()').get(default="").strip()
        return val if (val and val != 'N/A') else None

    def _extract_reach(self, container):
        val = container.xpath('.//strong[contains(text(), "Reach")]/ancestor::div/following-sibling::div[1]/span/text()').get(default="").strip()
        return val if val else None
