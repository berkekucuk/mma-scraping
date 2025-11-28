from ..utils.date_parser import DateParser
from ..utils.record_parser import RecordParser
from ..utils.url_parser import UrlParser
from ..utils.measurement_parser import MeasurementParser
from ..utils.weight_class_mapper import WeightClassMapper
from ..items import FighterItem


class FighterPageParser:

    @staticmethod
    def parse_fighter_profile(response, fighter_id, name, profile_url, image_url):

        header = response.css("div#fighterPageHeader")
        container = response.css("div#standardDetails")

        nickname = FighterPageParser.extract_detail(container, "Nickname:")

        record_str = FighterPageParser.extract_detail(container, "Pro MMA Record:")
        record = RecordParser.parse(record_str)

        date_of_birth_str = FighterPageParser.extract_detail(container, "Date of Birth:")
        date_of_birth = DateParser.parse_date_to_iso(date_of_birth_str)

        height_str = FighterPageParser.extract_detail(container, "Height:")
        height = MeasurementParser.parse_measurement(height_str)

        reach_str = FighterPageParser.extract_reach(container)
        reach = MeasurementParser.parse_measurement(reach_str)

        weight_class_name = FighterPageParser.extract_detail(container, "Weight Class:")
        weight_class_id = WeightClassMapper.map_weight_class(weight_class_name)
        born = FighterPageParser.extract_detail(container, "Born:")
        fighting_out_of = FighterPageParser.extract_detail(container, "Fighting out of:")
        style = FighterPageParser.extract_detail(container, "Foundation Style:")

        country_flag_relative_url = header.css("img::attr(src)").get(default="").strip() or None
        country_flag_url = response.urljoin(country_flag_relative_url) if country_flag_relative_url else None
        country_code = UrlParser.extract_country_code(country_flag_url) if country_flag_url else None

        fighter_item = FighterItem()
        fighter_item["item_type"] = "fighter"
        fighter_item["fighter_id"] = fighter_id
        fighter_item["name"] = name
        fighter_item["nickname"] = nickname
        fighter_item["record"] = record
        fighter_item["date_of_birth"] = date_of_birth
        fighter_item["height"] = height
        fighter_item["reach"] = reach
        fighter_item["weight_class_id"] = weight_class_id
        fighter_item["born"] = born
        fighter_item["fighting_out_of"] = fighting_out_of
        fighter_item["style"] = style
        fighter_item["country_code"] = country_code
        fighter_item["profile_url"] = profile_url
        fighter_item["image_url"] = image_url

        yield fighter_item

    @staticmethod
    def extract_detail(container, label):
        value = container.xpath(f'.//strong[contains(text(), "{label}")]/following-sibling::span[1]/text()').get(default="").strip()
        return value if (value and value != 'N/A') else None

    @staticmethod
    def extract_reach(container):
        value = (
            container.xpath('.//strong[contains(text(), "Reach")]/ancestor::div/following-sibling::div[1]/span/text()')
            .get(default="")
            .strip()
        )
        return value if value else None
