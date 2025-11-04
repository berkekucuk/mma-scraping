from .base_fight_parser import BaseFightParser


class UpcomingFightParser(BaseFightParser):

    @staticmethod
    def parse_fights(response, event_id):
        fights = response.css('ul[data-event-view-toggle-target="list"] > li[data-controller="table-row-background"]')

        # Parse normal fights
        for fight in fights:
            yield from UpcomingFightParser.parse_single_fight(fight, response, event_id)

        # Parse cancelled fights
        yield from UpcomingFightParser.parse_cancelled_fights(response, event_id)

    @staticmethod
    def parse_cancelled_fights(response, event_id):
        cancelled_fights_divs = response.xpath('//div[starts-with(@id, "bout") and contains(@id, "Cancelled")]')
        for cancelled_fight in cancelled_fights_divs:
            yield from BaseFightParser.handle_cancelled_fight(cancelled_fight, response, event_id)

    @staticmethod
    def parse_single_fight(fight, response, event_id):
        web_view = fight.xpath('./div[1]')

        # Fighter bilgileri
        fight_participants_div = web_view.xpath('./div[1]')
        fighter1_div = fight_participants_div.xpath('./div[1]')
        fighter2_div = fight_participants_div.xpath('./div[3]')

        fighter1_data = BaseFightParser.parse_fighter_info(fighter1_div, response, is_first_fighter=True)
        fighter2_data = BaseFightParser.parse_fighter_info(fighter2_div, response, is_first_fighter=False)

        # Fight detayları
        middle_div = fight_participants_div.xpath('./div[2]')
        box_div = middle_div.xpath('./div[1]')
        fight_data = BaseFightParser.parse_fight_details(box_div)

        bout_details_button_div = middle_div.xpath('./div[2]')
        fight_data['fight_order'] = bout_details_button_div.xpath('.//span[2]/text()').get(default='').strip()

        # Odds ve yaş bilgileri
        bout_details_div = web_view.xpath('./div[2]')
        odds_data = BaseFightParser.parse_odds(bout_details_div)
        ages_data = BaseFightParser.parse_ages(bout_details_div)

        # Items'ları yield et
        yield BaseFightParser.create_fight_item(fight_data, event_id)

        for fighter_item in BaseFightParser.create_fighter_items(fighter1_data, fighter2_data):
            yield fighter_item

        for participation_item in BaseFightParser.create_participation_items(
            fight_data['fight_id'], fighter1_data, fighter2_data, odds_data, ages_data,
            result1="pending", result2="pending"
        ):
            yield participation_item
