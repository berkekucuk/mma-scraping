from ..utils.fighter_age_util import FighterAgeUtil
from ..utils.url_util import URLUtil
from ..utils.fight_result_util import FightResultUtil
from ..utils.method_util import MethodUtil
from ..utils.odds_util import OddsUtil
from ..items import FightItem, FightParticipationItem, FighterItem
import scrapy

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
        method_str = first_div.css('span.uppercase::text').get(default='').strip()
        method_parsed = MethodUtil.split_method(method_str)
        method_type = method_parsed["method_type"]
        method_detail = method_parsed["method_detail"]
        round_summary = first_div.css(r'span.text-xs11.md\:text-xs10.leading-relaxed::text').get(default='').strip()

        #####################################################################################################################
        second_div = web_view.xpath('./div[2]')

        fighter1_div = second_div.xpath('./div[1]')
        fighter1_name = fighter1_div.css('a.link-primary-red::text').get(default='').strip()
        fighter1_relative_url = fighter1_div.css('a.link-primary-red::attr(href)').get(default='').strip()
        fighter1_url = response.urljoin(fighter1_relative_url) if fighter1_relative_url else ''
        fighter1_id = URLUtil.extract_fighter_id(fighter1_url) if fighter1_url else None
        fighter1_image_url = fighter1_div.css('div.relative.order-first img::attr(src)').get(default='').strip()
        fighter1_result = FightResultUtil.determine_fight_result(fighter1_div)

        middle_div = second_div.xpath('./div[2]')
        box_div = middle_div.xpath('./div[1]')

        card_section = box_div.xpath('./span[1]/a/text()').get(default='').strip()
        weight_class_lbs = box_div.xpath('./div[1]/span/text()').get(default='').strip()
        rounds_format = box_div.xpath('./div[2]/text()').get(default='').strip()

        bout_details_div = middle_div.xpath('./div[2]')
        card_number = bout_details_div.xpath('.//span[2]/text()').get(default='').strip()

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

        odds_row = table.xpath(".//tr[td[contains(., 'Betting Odds')]]").get()

        if odds_row:
            row_sel = scrapy.Selector(text=odds_row)
            odds_texts = row_sel.css('div.hidden.md\\:inline::text').getall()
            odds_texts = [o.strip() for o in odds_texts if o.strip()]

            fighter1_odds = odds_texts[0] if len(odds_texts) > 0 else None
            fighter2_odds = odds_texts[1] if len(odds_texts) > 1 else None

            # OddsUtil yardımıyla ayrıştır
            f1_parsed = OddsUtil.split_odds(fighter1_odds)
            f2_parsed = OddsUtil.split_odds(fighter2_odds)

            fighter1_odds_value = f1_parsed["odds_value"]
            fighter1_odds_label = f1_parsed["odds_label"]
            fighter2_odds_value = f2_parsed["odds_value"]
            fighter2_odds_label = f2_parsed["odds_label"]

        else:
            fighter1_odds_value = fighter1_odds_label = None
            fighter2_odds_value = fighter2_odds_label = None

        ages = table.css('td.text-neutral-950::text').getall()
        ages = [age.strip() for age in ages if 'years' in age]
        fighter1_age_at_fight = FighterAgeUtil.parse_fighter_age(ages[0]) if len(ages) > 0 else None
        fighter2_age_at_fight = FighterAgeUtil.parse_fighter_age(ages[1]) if len(ages) > 1 else None

        #####################################################################################################################
        # ---- Fight Item ----
        fight_item = FightItem()
        fight_item['fight_id'] = fight_id
        fight_item['event_id'] = event_id
        fight_item['method_type'] = method_type
        fight_item['method_detail'] = method_detail
        fight_item['round_summary'] = round_summary
        fight_item['card_section'] = card_section
        fight_item['weight_class_lbs'] = weight_class_lbs
        fight_item['rounds_format'] = rounds_format
        fight_item['card_number'] = card_number
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
        for id, odds_value, odds_label, age, result in [
            (fighter1_id, fighter1_odds_value, fighter1_odds_label, fighter1_age_at_fight, fighter1_result),
            (fighter2_id, fighter2_odds_value, fighter2_odds_label, fighter2_age_at_fight, fighter2_result)
        ]:
            participation = FightParticipationItem()
            participation['fight_id'] = fight_id
            participation['fighter_id'] = id
            participation['odds_value'] = odds_value
            participation['odds_label'] = odds_label
            participation['age_at_fight'] = age
            participation['result'] = result
            yield participation
