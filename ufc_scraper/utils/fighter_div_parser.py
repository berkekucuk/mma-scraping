from .result_parser import ResultParser
from .url_parser import UrlParser


class FighterDivParser:

    @staticmethod
    def parse_fighter_div(fighter_div, response, is_first_fighter=True):

        css_class = "order-first" if is_first_fighter else "order-last"

        name = fighter_div.css("a.link-primary-red::text").get(default="").strip()
        relative_url = fighter_div.css("a.link-primary-red::attr(href)").get(default="").strip()
        profile_url = response.urljoin(relative_url) if relative_url else ""
        fighter_id = UrlParser.extract_fighter_id(profile_url) if profile_url else None
        image_url = fighter_div.css(f"div.relative.{css_class} img::attr(src)").get(default="").strip()
        result = ResultParser.determine_fight_result(fighter_div)

        record_after_fight = None

        if result != "pending":
            record_after_fight_string = fighter_div.xpath(
            './/span[contains(@class, "text-[15px]") and contains(@class, "md:text-xs") and contains(@class, "leading-tight")]/text()'
            ).get(default="").strip()
            record_after_fight = FighterDivParser.parse_record_string(record_after_fight_string)

        return {
            "fighter_id": fighter_id,
            "name": name,
            "profile_url": profile_url,
            "image_url": image_url,
            "result": result,
            "record_after_fight": record_after_fight,
        }

    @staticmethod
    def parse_record_string(record_string):

        if not record_string:
            return {"wins": 0, "losses": 0, "draws": 0}

        parts = record_string.split("-")

        wins = int(parts[0]) if len(parts) > 0 and parts[0].isdigit() else 0
        losses = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0
        draws = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 0

        return {"wins": wins, "losses": losses, "draws": draws}
