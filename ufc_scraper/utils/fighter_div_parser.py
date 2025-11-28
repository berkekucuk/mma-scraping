from .result_parser import ResultParser
from .url_parser import UrlParser
from .record_parser import RecordParser


class FighterDivParser:

    @staticmethod
    def parse_fighter_div(fighter_div, response, is_first_fighter=True):

        css_class = "order-first" if is_first_fighter else "order-last"

        name = fighter_div.css("a.link-primary-red::text").get(default="").strip() or None
        relative_url = fighter_div.css("a.link-primary-red::attr(href)").get(default="").strip() or None
        profile_url = response.urljoin(relative_url) if relative_url else None
        fighter_id = UrlParser.extract_fighter_id(relative_url) if relative_url else None
        image_url = fighter_div.css(f"div.relative.{css_class} img::attr(src)").get(default="").strip() or None
        result = ResultParser.determine_fight_result(fighter_div)

        record_after_fight = None

        if result != "pending":
            record_after_fight_str = fighter_div.xpath('.//span[contains(@class, "text-[15px]") and contains(@class, "md:text-xs") and contains(@class, "leading-tight")]/text()').get(default="").strip() or None

            if record_after_fight_str:
                record_after_fight = RecordParser.parse(record_after_fight_str)

        return {
            "fighter_id": fighter_id,
            "name": name,
            "profile_url": profile_url,
            "image_url": image_url,
            "result": result,
            "record_after_fight": record_after_fight,
        }

