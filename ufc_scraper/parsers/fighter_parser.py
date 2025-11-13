from ..utils.date_parser import DateParser
from ..utils.record_parser import RecordParser
from ..utils.country_parser import CountryParser
from ..utils.measurement_parser import MeasurementParser


class FighterParser:

    @staticmethod
    def parse_fighter(response):
        header = response.css("div#fighterPageHeader")
        container = response.css("div#standardDetails")

        nickname = FighterParser.extract_detail(container, "Nickname:")

        record_string = FighterParser.extract_detail(container, "Pro MMA Record:")
        record = RecordParser.parse_record(record_string)

        date_of_birth_string = FighterParser.extract_detail(container, "Date of Birth:")
        date_of_birth = DateParser.parse_date_to_iso(date_of_birth_string)

        height_string = FighterParser.extract_detail(container, "Height:")
        height = MeasurementParser.parse_measurement(height_string)

        reach_string = FighterParser.extract_reach(container)
        reach = MeasurementParser.parse_measurement(reach_string)

        weight_class_name = FighterParser.extract_detail(container, "Weight Class:") or None
        born = FighterParser.extract_detail(container, "Born:") or None
        fighting_out_of = FighterParser.extract_detail(container, "Fighting out of:") or None
        style = FighterParser.extract_detail(container, "Foundation Style:") or None

        country_flag_relative_url = header.css("img::attr(src)").get(default="").strip()
        country_flag_url = response.urljoin(country_flag_relative_url)
        country_code = CountryParser.extract_country_src(country_flag_url)

        if country_flag_url and country_code:
            CountryParser.save_country_to_json(country_code, country_flag_url)

        fighter_data = {
            "nickname": nickname,
            "record": record,
            "date_of_birth": date_of_birth,
            "height": height,
            "reach": reach,
            "weight_class_name": weight_class_name,
            "born": born,
            "fighting_out_of": fighting_out_of,
            "style": style,
            "country_code": country_code,
        }

        return fighter_data

    @staticmethod
    def extract_detail(container, label):
        value = container.xpath(
            f'//strong[contains(text(), "{label}")]/following-sibling::span[1]/text()'
        ).get(default="").strip()
        return value if value else None

    @staticmethod
    def extract_reach(container):
        value = (
            container.xpath('//strong[contains(text(), "Reach")]/ancestor::div/following-sibling::div[1]/span/text()')
            .get(default="")
            .strip()
        )
        return value if value else None

