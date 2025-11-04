import scrapy
from ..items import FightItem, FightParticipationItem, FighterItem
from ..utils.fighter_age_util import FighterAgeUtil
from ..utils.odds_util import OddsUtil
from ..utils.url_util import URLUtil


class BaseFightParser:

    @staticmethod
    def parse_fighter_info(fighter_div, response, is_first_fighter=True):

        css_class = 'order-first' if is_first_fighter else 'order-last'

        name = fighter_div.css('a.link-primary-red::text').get(default='').strip()
        relative_url = fighter_div.css('a.link-primary-red::attr(href)').get(default='').strip()
        profile_url = response.urljoin(relative_url) if relative_url else ''
        fighter_id = URLUtil.extract_fighter_id(profile_url) if profile_url else None
        image_url = fighter_div.css(f'div.relative.{css_class} img::attr(src)').get(default='').strip()

        return {
            'fighter_id': fighter_id,
            'name': name,
            'profile_url': profile_url,
            'image_url': image_url
        }

    @staticmethod
    def parse_fight_details(box_div):

        fight_relative_url = box_div.xpath('./span[1]/a/@href').get(default='').strip()
        fight_id = URLUtil.extract_fight_id(fight_relative_url)
        bout_type = box_div.xpath('./span[1]/a/text()').get(default='').strip()
        weight_class_lbs = box_div.xpath('./div[1]/span/text()').get(default='').strip()
        rounds_format = box_div.xpath('./div[2]/text()').get(default='').strip()

        return {
            'fight_id': fight_id,
            'bout_type': bout_type,
            'weight_class_lbs': weight_class_lbs,
            'rounds_format': rounds_format
        }

    @staticmethod
    def parse_odds(bout_details_div):

        table = bout_details_div.css('table#boutComparisonTable')
        odds_row = table.xpath(".//tr[td[contains(., 'Betting Odds')]]").get()

        if odds_row:
            row_sel = scrapy.Selector(text=odds_row)
            odds_texts = row_sel.css('div.hidden.md\\:inline::text').getall()
            odds_texts = [o.strip() for o in odds_texts if o.strip()]

            fighter1_odds = odds_texts[0] if len(odds_texts) > 0 else None
            fighter2_odds = odds_texts[1] if len(odds_texts) > 1 else None

            f1_parsed = OddsUtil.split_odds(fighter1_odds)
            f2_parsed = OddsUtil.split_odds(fighter2_odds)

            return {
                'fighter1_odds_value': f1_parsed["odds_value"],
                'fighter1_odds_label': f1_parsed["odds_label"],
                'fighter2_odds_value': f2_parsed["odds_value"],
                'fighter2_odds_label': f2_parsed["odds_label"]
            }
        else:
            return {
                'fighter1_odds_value': None,
                'fighter1_odds_label': None,
                'fighter2_odds_value': None,
                'fighter2_odds_label': None
            }

    @staticmethod
    def parse_ages(bout_details_div):

        table = bout_details_div.css('table#boutComparisonTable')
        ages = table.css('td.text-neutral-950::text').getall()
        ages = [age.strip() for age in ages if 'years' in age]

        return {
            'fighter1_age': FighterAgeUtil.parse_fighter_age(ages[0]) if len(ages) > 0 else None,
            'fighter2_age': FighterAgeUtil.parse_fighter_age(ages[1]) if len(ages) > 1 else None
        }

    @staticmethod
    def parse_cancelled_fight(cancelled_fight_div, response):
        # Sol dövüşçü
        fighter1_name =  cancelled_fight_div.xpath('.//div[@id="leftNdesktop"]//a/text()').get()
        fighter1_relative_url = cancelled_fight_div.xpath('.//div[@id="leftNdesktop"]//a/@href').get()
        fighter1_profile_url = response.urljoin(fighter1_relative_url) if fighter1_relative_url else ''
        fighter1_id = URLUtil.extract_fighter_id(fighter1_relative_url) if fighter1_relative_url else None
        fighter1_img = cancelled_fight_div.xpath('.//div[1]//img/@src').get()

        # Sağ dövüşçü
        fighter2_name = cancelled_fight_div.xpath('.//div[@id="rightNdesktop"]//a/text()').get()
        fighter2_relative_url = cancelled_fight_div.xpath('.//div[@id="rightNdesktop"]//a/@href').get()
        fighter2_profile_url = response.urljoin(fighter2_relative_url) if fighter2_relative_url else ''
        fighter2_id = URLUtil.extract_fighter_id(fighter2_relative_url) if fighter2_relative_url else None
        fighter2_img = cancelled_fight_div.xpath('.//div[@id="rightNdesktop"]/following-sibling::div//img/@src').get()

        # Orta kısım: durum ve cancelled bilgisi
        middle_div = cancelled_fight_div.xpath('.//div[@data-controller="tooltip"]')
        status_text = middle_div.xpath('.//a/text()').get()
        status_reason = middle_div.xpath('.//span[contains(@class, "text-sm")]/text()').get()
        fight_relative_url = middle_div.xpath('.//a/@href').get()
        fight_id = URLUtil.extract_fight_id(fight_relative_url) if fight_relative_url else None

        return {
            'fighter1_data': {
                'fighter_id': fighter1_id,
                'name': fighter1_name,
                'profile_url': fighter1_profile_url,
                'image_url': fighter1_img
            },
            'fighter2_data': {
                'fighter_id': fighter2_id,
                'name': fighter2_name,
                'profile_url': fighter2_profile_url,
                'image_url': fighter2_img
                },
            'fight_data': {
                'status_text': status_text,
                'status_reason': status_reason,
                'fight_id': fight_id
                }
            }

    @staticmethod
    def handle_cancelled_fight(cancelled_fight_div, response, event_id):
        cancelled_fight_data = BaseFightParser.parse_cancelled_fight(cancelled_fight_div, response)

        fighter1_data = cancelled_fight_data['fighter1_data']
        fighter2_data = cancelled_fight_data['fighter2_data']
        details = cancelled_fight_data['fight_data']

        fight_data = {
            'fight_id': details['fight_id']
        }

        # FightItem oluştur
        yield BaseFightParser.create_fight_item(fight_data, event_id, method_type=details['status_text'], method_detail=details['status_reason'])

        # FighterItem'ları oluştur
        for fighter_item in BaseFightParser.create_fighter_items(fighter1_data, fighter2_data):
            yield fighter_item

        for participation_item in BaseFightParser.create_participation_items(fight_data['fight_id'], fighter1_data, fighter2_data, result1=details['status_text'], result2=details['status_text']):
            yield participation_item


    @staticmethod
    def create_fight_item(fight_data, event_id, method_type="", method_detail="", round_summary=""):

        fight_item = FightItem()
        fight_item['fight_id'] = fight_data['fight_id']
        fight_item['event_id'] = event_id
        fight_item['method_type'] = method_type
        fight_item['method_detail'] = method_detail
        fight_item['round_summary'] = round_summary
        fight_item['bout_type'] = fight_data.get('bout_type', '')
        fight_item['weight_class_lbs'] = fight_data.get('weight_class_lbs', '')
        fight_item['rounds_format'] = fight_data.get('rounds_format', '')
        fight_item['fight_order'] = fight_data.get('fight_order', '')
        return fight_item

    @staticmethod
    def create_fighter_items(fighter1_data, fighter2_data):

        items = []
        for fighter_data in [fighter1_data, fighter2_data]:
            fighter_item = FighterItem()
            fighter_item['fighter_id'] = fighter_data['fighter_id']
            fighter_item['name'] = fighter_data['name']
            fighter_item['profile_url'] = fighter_data['profile_url']
            fighter_item['image_url'] = fighter_data['image_url']
            items.append(fighter_item)
        return items

    @staticmethod
    def create_participation_items(fight_id, fighter1_data, fighter2_data, odds_data=None, ages_data=None, result1="", result2=""):

        odds_data = odds_data or {}
        ages_data = ages_data or {}

        items = []
        for fighter_data, odds_value, odds_label, age, result in [
            (fighter1_data, odds_data.get('fighter1_odds_value', None), odds_data.get('fighter1_odds_label', None), ages_data.get('fighter1_age', None), result1),
            (fighter2_data, odds_data.get('fighter2_odds_value', None), odds_data.get('fighter2_odds_label', None), ages_data.get('fighter2_age', None), result2)
        ]:
            participation = FightParticipationItem()
            participation['fight_id'] = fight_id
            participation['fighter_id'] = fighter_data['fighter_id']
            participation['odds_value'] = odds_value
            participation['odds_label'] = odds_label
            participation['age_at_fight'] = age
            participation['result'] = result
            items.append(participation)
        return items

