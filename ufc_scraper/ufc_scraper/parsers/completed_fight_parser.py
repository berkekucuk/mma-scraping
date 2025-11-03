from .base_fight_parser import BaseFightParser
from ..utils.fight_result_util import FightResultUtil
from ..utils.method_util import MethodUtil


class CompletedFightParser(BaseFightParser):

    @staticmethod
    def parse_fights(response, event_id):
        fights = response.css('ul[data-event-view-toggle-target="list"] > li[data-controller="table-row-background"]')
        for fight in fights:
            for item in CompletedFightParser.parse_single_fight(fight, response, event_id):
                yield item

    @staticmethod
    def parse_single_fight(fight, response, event_id):
        web_view = fight.xpath('./div[1]')

        # Method bilgisi (sadece completed fights'ta var)
        fight_summary_div = web_view.xpath('./div[1]')
        method_str = fight_summary_div.css('span.uppercase::text').get(default='').strip()
        method_parsed = MethodUtil.split_method(method_str)
        round_summary = fight_summary_div.css(r'span.text-xs11.md\:text-xs10.leading-relaxed::text').get(default='').strip()

        # Fighter bilgileri
        fight_participants_div = web_view.xpath('./div[2]')
        fighter1_div = fight_participants_div.xpath('./div[1]')
        fighter2_div = fight_participants_div.xpath('./div[3]')

        fighter1_data = BaseFightParser.parse_fighter_info(fighter1_div, response, is_first_fighter=True)
        fighter2_data = BaseFightParser.parse_fighter_info(fighter2_div, response, is_first_fighter=False)

        # Result bilgileri (sadece completed fights'ta var)
        fighter1_result = FightResultUtil.determine_fight_result(fighter1_div)
        fighter2_result = FightResultUtil.determine_fight_result(fighter2_div)

        # Fight detayları
        middle_div = fight_participants_div.xpath('./div[2]')
        box_div = middle_div.xpath('./div[1]')
        fight_data = BaseFightParser.parse_fight_details(box_div)

        bout_details_button_div = middle_div.xpath('./div[2]')
        fight_data['fight_order'] = bout_details_button_div.xpath('.//span[2]/text()').get(default='').strip()

        # Odds ve yaş bilgileri
        bout_details_div = web_view.xpath('./div[3]')
        odds_data = BaseFightParser.parse_odds(bout_details_div)
        ages_data = BaseFightParser.parse_ages(bout_details_div)

        # Items'ları yield et
        yield BaseFightParser.create_fight_item(
            fight_data, event_id,
            method_type=method_parsed["method_type"],
            method_detail=method_parsed["method_detail"],
            round_summary=round_summary
        )

        for fighter_item in BaseFightParser.create_fighter_items(fighter1_data, fighter2_data):
            yield fighter_item

        for participation_item in BaseFightParser.create_participation_items(
            fight_data['fight_id'], fighter1_data, fighter2_data, odds_data, ages_data,
            result1=fighter1_result, result2=fighter2_result
        ):
            yield participation_item
