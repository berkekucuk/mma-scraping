import scrapy

from ..utils.fight_result_util import FightResultUtil
from ..items import FightItem, FightParticipationItem, FighterItem
from ..utils.fighter_age_util import FighterAgeUtil
from ..utils.odds_util import OddsUtil
from ..utils.url_util import URLUtil


class BaseFightParser:

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

    @staticmethod
    def parse_fight_metadata(box_div):

        fight_relative_url = box_div.xpath("./span[1]/a/@href").get(default="").strip()
        fight_id = URLUtil.extract_fight_id(fight_relative_url)
        bout_type = box_div.xpath("./span[1]/a/text()").get(default="").strip()
        weight_class_lbs = box_div.xpath("./div[1]/span/text()").get(default="").strip()
        rounds_format = box_div.xpath("./div[2]/text()").get(default="").strip()

        return {
            "fight_id": fight_id,
            "bout_type": bout_type,
            "weight_class_lbs": weight_class_lbs,
            "rounds_format": rounds_format,
        }

    @staticmethod
    def parse_odds(bout_details_div):

        table = bout_details_div.css("table#boutComparisonTable")
        odds_row = table.xpath(".//tr[td[contains(., 'Betting Odds')]]").get()

        if odds_row:
            row_sel = scrapy.Selector(text=odds_row)
            odds_texts = row_sel.css("div.hidden.md\\:inline::text").getall()
            odds_texts = [o.strip() for o in odds_texts if o.strip()]

            fighter1_odds = odds_texts[0] if len(odds_texts) > 0 else None
            fighter2_odds = odds_texts[1] if len(odds_texts) > 1 else None

            f1_parsed = OddsUtil.split_odds(fighter1_odds)
            f2_parsed = OddsUtil.split_odds(fighter2_odds)

            return {
                "fighter1_odds_value": f1_parsed["odds_value"],
                "fighter1_odds_label": f1_parsed["odds_label"],
                "fighter2_odds_value": f2_parsed["odds_value"],
                "fighter2_odds_label": f2_parsed["odds_label"],
            }
        else:
            return {
                "fighter1_odds_value": None,
                "fighter1_odds_label": None,
                "fighter2_odds_value": None,
                "fighter2_odds_label": None,
            }

    @staticmethod
    def parse_ages(bout_details_div):

        table = bout_details_div.css("table#boutComparisonTable")
        ages = table.css("td.text-neutral-950::text").getall()
        ages = [age.strip() for age in ages if "years" in age]

        return {
            "fighter1_age": (FighterAgeUtil.parse_fighter_age(ages[0]) if len(ages) > 0 else None),
            "fighter2_age": (FighterAgeUtil.parse_fighter_age(ages[1]) if len(ages) > 1 else None),
        }

    @staticmethod
    def create_fight_item(fight_metadata, event_id, method_type="", method_detail="", round_summary=""):

        fight_item = FightItem()
        fight_item["fight_id"] = fight_metadata["fight_id"]
        fight_item["event_id"] = event_id
        fight_item["method_type"] = method_type
        fight_item["method_detail"] = method_detail
        fight_item["round_summary"] = round_summary
        fight_item["bout_type"] = fight_metadata.get("bout_type", "")
        fight_item["weight_class_lbs"] = fight_metadata.get("weight_class_lbs", "")
        fight_item["rounds_format"] = fight_metadata.get("rounds_format", "")
        fight_item["fight_order"] = fight_metadata.get("fight_order", "")
        return fight_item

    @staticmethod
    def create_fighter_items(fighter1_data, fighter2_data):

        items = []
        for fighter_data in [fighter1_data, fighter2_data]:
            fighter_item = FighterItem()
            fighter_item["fighter_id"] = fighter_data["fighter_id"]
            fighter_item["name"] = fighter_data["name"]
            fighter_item["profile_url"] = fighter_data["profile_url"]
            fighter_item["image_url"] = fighter_data["image_url"]
            items.append(fighter_item)
        return items

    @staticmethod
    def create_participation_items(
        fight_id,
        fighter1_data,
        fighter2_data,
        odds_data=None,
        ages_data=None,
        result1=None,
        result2=None,
    ):

        odds_data = odds_data or {}
        ages_data = ages_data or {}

        items = []
        for fighter_data, odds_value, odds_label, age, result in [
            (
                fighter1_data,
                odds_data.get("fighter1_odds_value", None),
                odds_data.get("fighter1_odds_label", None),
                ages_data.get("fighter1_age", None),
                result1 or fighter1_data.get("result"),
            ),
            (
                fighter2_data,
                odds_data.get("fighter2_odds_value", None),
                odds_data.get("fighter2_odds_label", None),
                ages_data.get("fighter2_age", None),
                result2 or fighter2_data.get("result"),
            ),
        ]:
            participation = FightParticipationItem()
            participation["fight_id"] = fight_id
            participation["fighter_id"] = fighter_data["fighter_id"]
            participation["odds_value"] = odds_value
            participation["odds_label"] = odds_label
            participation["age_at_fight"] = age
            participation["result"] = result
            items.append(participation)
        return items
