from .fight_result_util import FightResultUtil
from .url_util import URLUtil


class FighterInfoUtil:

    @staticmethod
    def parse_fighter_info(fighter_div, response, is_first_fighter=True):

        css_class = "order-first" if is_first_fighter else "order-last"

        name = fighter_div.css("a.link-primary-red::text").get(default="").strip()
        relative_url = fighter_div.css("a.link-primary-red::attr(href)").get(default="").strip()
        profile_url = response.urljoin(relative_url) if relative_url else ""
        fighter_id = URLUtil.extract_fighter_id(profile_url) if profile_url else None
        image_url = fighter_div.css(f"div.relative.{css_class} img::attr(src)").get(default="").strip()
        result = FightResultUtil.determine_fight_result(fighter_div)

        return {
            "fighter_id": fighter_id,
            "name": name,
            "profile_url": profile_url,
            "image_url": image_url,
            "result": result,
        }
