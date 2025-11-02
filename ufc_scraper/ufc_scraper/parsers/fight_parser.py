from ..utils.fighter_age_util import FighterAgeUtil
from ..utils.url_util import URLUtil
from ..utils.fight_result_util import FightResultUtil
from ..items import FightItem, FightParticipationItem, FighterItem

class FightParser:

    @staticmethod
    def parse_fights(response, event_id):
        fights = response.css('ul[data-event-view-toggle-target="list"] > li[data-controller="table-row-background"]')

        for fight in fights:
            for item in FightParser.parse_single_fight(fight, response, event_id):
                yield item

    @staticmethod
    def parse_single_fight(fight, response, event_id):
        web_view = fight.xpath('./div[1]')

        #####################################################################################################################
        first_div = web_view.xpath('./div[1]')
        method = first_div.css('span.uppercase::text').get(default='').strip()
        round_info = first_div.css('span.text-xs11.md\:text-xs10.leading-relaxed::text').get(default='').strip()

        #####################################################################################################################
        second_div = web_view.xpath('./div[2]')

        fighter1_div = second_div.xpath('./div[1]')
        fighter1_name = fighter1_div.css('a.link-primary-red::text').get(default='').strip()
        fighter1_relative_url = fighter1_div.css('a.link-primary-red::attr(href)').get(default='').strip()
        fighter1_url = response.urljoin(fighter1_relative_url) if fighter1_relative_url else ''
        fighter1_id = URLUtil.extract_fighter_id(fighter1_url) if fighter1_url else None
        fighter1_image_url = fighter1_div.css('div.relative.order-first img::attr(src)').get(default='').strip()
        fighter1_result = FightResultUtil.determine_fight_result(fighter1_div)

        fight_info_div = second_div.xpath('./div[2]')
        fight_info_div_child1 = fight_info_div.xpath('./div[1]')
        weight_class = fight_info_div_child1.css('span.bg-tap_darkgold::text').get(default='').strip()

        fight_info_div_child2 = fight_info_div.xpath('./div[2]')
        fight_number = fight_info_div_child2.xpath('.//span[2]/text()').get(default='').strip()

        fighter2_div = second_div.xpath('./div[3]')
        fighter2_name = fighter2_div.css('a.link-primary-red::text').get(default='').strip()
        fighter2_relative_url = fighter2_div.css('a.link-primary-red::attr(href)').get(default='').strip()
        fighter2_url = response.urljoin(fighter2_relative_url) if fighter2_relative_url else ''
        fighter2_id = URLUtil.extract_fighter_id(fighter2_url) if fighter2_url else None
        fighter2_image_url = fighter2_div.css('div.relative.order-last img::attr(src)').get(default='').strip()
        fighter2_result = FightResultUtil.determine_fight_result(fighter2_div)

        #####################################################################################################################
        third_div = web_view.xpath('./div[3]')
        fight_relative_url = third_div.css('a::attr(href)').get(default='').strip()
        fight_id = URLUtil.extract_fight_id(fight_relative_url)

        table = third_div.css('table#boutComparisonTable')

        odds_row = table.xpath(".//tr[td[contains(., 'Betting Odds')]]")
        if odds_row:
            odds_texts = odds_row.css('div.hidden.md\\:inline::text').getall()
            odds_texts = [o.strip() for o in odds_texts if o.strip()]

            fighter1_odds = odds_texts[0] if len(odds_texts) > 0 else None
            fighter2_odds = odds_texts[-1] if len(odds_texts) > 1 else None
        else:
            fighter1_odds = fighter2_odds = None

        ages = table.css('td.text-neutral-950::text').getall()
        ages = [age.strip() for age in ages if 'years' in age]
        fighter1_age_at_fight = FighterAgeUtil.parse_fighter_age(ages[0]) if len(ages) > 0 else None
        fighter2_age_at_fight = FighterAgeUtil.parse_fighter_age(ages[1]) if len(ages) > 1 else None

        #####################################################################################################################
        # ---- Fight Item ----
        fight_item = FightItem()
        fight_item['fight_id'] = fight_id
        fight_item['event_id'] = event_id
        fight_item['weight_class'] = weight_class
        fight_item['method'] = method
        fight_item['round_info'] = round_info
        fight_item['fight_number'] = fight_number
        yield fight_item

        # ---- Fighter Item'lar ----
        for id, name, url, img in [
            (fighter1_id, fighter1_name, fighter1_url, fighter1_image_url),
            (fighter2_id, fighter2_name, fighter2_url, fighter2_image_url)
        ]:
            fighter_item = FighterItem()
            fighter_item['fighter_id'] = id
            fighter_item['name'] = name
            fighter_item['profile_url'] = url
            fighter_item['image_url'] = img
            yield fighter_item

        # ---- FightParticipation (bağlantı tablosu) ----
        for id, odds, age, result in [
            (fighter1_id, fighter1_odds, fighter1_age_at_fight, fighter1_result),
            (fighter2_id, fighter2_odds, fighter2_age_at_fight, fighter2_result)
        ]:
            participation = FightParticipationItem()
            participation['fight_id'] = fight_id
            participation['fighter_id'] = id
            participation['odds'] = odds
            participation['age_at_fight'] = age
            participation['fight_result'] = result
            yield participation
