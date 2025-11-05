from ..utils.url_util import URLUtil
from .base_fight_parser import BaseFightParser
from ..utils.method_util import MethodUtil


class FightParser(BaseFightParser):

    @staticmethod
    def parse_fights(response, event_id):

        fights = response.css('ul[data-event-view-toggle-target="list"] > li[data-controller="table-row-background"]')

        cancelled_fights = response.xpath('//div[starts-with(@id, "bout") and contains(@id, "Cancelled")]')

        # Parse normal fights
        for fight in fights:
            yield from FightParser.parse_single_fight(fight, response, event_id)

        # Parse cancelled fights
        for cancelled_fight in cancelled_fights:
            yield from FightParser.parse_cancelled_fight(cancelled_fight, response, event_id)

    @staticmethod
    def parse_single_fight(fight, response, event_id):
        web_view = fight.xpath("./div[1]")

        # Method bilgisi (sadece completed fights'ta var)
        fight_summary_div = web_view.xpath(".//div[contains(@class, 'flex w-full mt-1 mb-0.5 px-1.5')]")
        method_str = fight_summary_div.css("span.uppercase::text").get(default="").strip()
        method_parsed = MethodUtil.split_method(method_str)
        round_summary = (
            fight_summary_div.css(r"span.text-xs11.md\:text-xs10.leading-relaxed::text").get(default="").strip()
        )

        # Fighter bilgileri
        fight_participants_div = web_view.xpath(
            "./div[@class='div group flex items:start justify-center gap-0.5 md:gap-0']"
        )
        fighter1_div = fight_participants_div.xpath("./div[1]")
        fighter2_div = fight_participants_div.xpath("./div[3]")

        fighter1_data = BaseFightParser.parse_fighter_info(fighter1_div, response, is_first_fighter=True)
        fighter2_data = BaseFightParser.parse_fighter_info(fighter2_div, response, is_first_fighter=False)

        # Fight metadatası
        middle_div = fight_participants_div.xpath("./div[2]")
        box_div = middle_div.xpath("./div[1]")
        fight_metadata = BaseFightParser.parse_fight_metadata(box_div)

        bout_details_button_div = middle_div.xpath("./div[2]")
        fight_metadata["fight_order"] = bout_details_button_div.xpath(".//span[2]/text()").get(default="").strip()

        # Odds ve yaş bilgileri
        bout_details_div = web_view.xpath("./div[@data-event-bout-details-target='content']")
        odds_data = BaseFightParser.parse_odds(bout_details_div)
        ages_data = BaseFightParser.parse_ages(bout_details_div)

        # Items'ları yield et
        yield BaseFightParser.create_fight_item(
            fight_metadata,
            event_id,
            method_type=method_parsed["method_type"],
            method_detail=method_parsed["method_detail"],
            round_summary=round_summary,
        )

        for fighter_item in BaseFightParser.create_fighter_items(fighter1_data, fighter2_data):
            yield fighter_item

        for participation_item in BaseFightParser.create_participation_items(
            fight_metadata["fight_id"],
            fighter1_data,
            fighter2_data,
            odds_data,
            ages_data,
        ):
            yield participation_item

    @staticmethod
    def parse_cancelled_fight(cancelled_fight_div, response, event_id):
        # Sol dövüşçü
        fighter1_name = cancelled_fight_div.xpath('.//div[@id="leftNdesktop"]//a/text()').get(default="").strip()
        fighter1_relative_url = cancelled_fight_div.xpath('.//div[@id="leftNdesktop"]//a/@href').get(default="").strip()
        fighter1_profile_url = response.urljoin(fighter1_relative_url) if fighter1_relative_url else ""
        fighter1_id = URLUtil.extract_fighter_id(fighter1_relative_url) if fighter1_relative_url else None
        fighter1_img = cancelled_fight_div.xpath(".//div[1]//img/@src").get(default="").strip()

        # Sağ dövüşçü
        fighter2_name = cancelled_fight_div.xpath('.//div[@id="rightNdesktop"]//a/text()').get(default="").strip()
        fighter2_relative_url = (
            cancelled_fight_div.xpath('.//div[@id="rightNdesktop"]//a/@href').get(default="").strip()
        )
        fighter2_profile_url = response.urljoin(fighter2_relative_url) if fighter2_relative_url else ""
        fighter2_id = URLUtil.extract_fighter_id(fighter2_relative_url) if fighter2_relative_url else None
        fighter2_img = (
            cancelled_fight_div.xpath('.//div[@id="rightNdesktop"]/following-sibling::div//img/@src')
            .get(default="")
            .strip()
        )

        # Orta kısım: durum ve cancelled bilgisi
        middle_div = cancelled_fight_div.xpath('.//div[@data-controller="tooltip"]')
        status_text = middle_div.xpath(".//a/text()").get(default="").strip()
        status_reason = middle_div.xpath('.//span[contains(@class, "text-sm")]/text()').get(default="").strip()
        fight_relative_url = middle_div.xpath(".//a/@href").get(default="").strip()
        fight_id = URLUtil.extract_fight_id(fight_relative_url) if fight_relative_url else None

        # Fighter data dictionaries
        fighter1_data = {
            "fighter_id": fighter1_id,
            "name": fighter1_name,
            "profile_url": fighter1_profile_url,
            "image_url": fighter1_img,
        }

        fighter2_data = {
            "fighter_id": fighter2_id,
            "name": fighter2_name,
            "profile_url": fighter2_profile_url,
            "image_url": fighter2_img,
        }

        fight_data = {"fight_id": fight_id}

        # Yield FightItem
        yield BaseFightParser.create_fight_item(
            fight_data,
            event_id,
            method_type=status_text,
            method_detail=status_reason,
        )

        # Yield FighterItems
        for fighter_item in BaseFightParser.create_fighter_items(fighter1_data, fighter2_data):
            yield fighter_item

        # Yield ParticipationItems
        for participation_item in BaseFightParser.create_participation_items(
            fight_id,
            fighter1_data,
            fighter2_data,
            result1=status_text,
            result2=status_text,
        ):
            yield participation_item
