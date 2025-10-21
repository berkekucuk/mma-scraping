from ..utils.url_utils import URLUtils
from ..utils.date_time_utils import DateTimeUtils
from ..utils.fight_result import FightResult

class CardParser:

    @staticmethod
    def parse_card(response):
        event_type = response.css('div#eventPageHeader span::text').get(default='').strip()
        event_name = response.css('h2::text').get(default='').strip()

        container = response.css('ul[data-controller="unordered-list-background"]')

        date_time_str = container.css('span:contains("Date/Time:") + span::text').get(default='').strip()
        date_time = DateTimeUtils.parse_tapology_datetime(date_time_str)
        
        venue = container.css('span:contains("Venue:") + span::text').get(default='').strip()
        location = container.css('span:contains("Location:") + span a::text').get(default='').strip()

        return {
            'event_type': event_type,
            'event_name': event_name,
            'date_time': date_time,  
            'venue': venue,
            'location': location
        }

    @staticmethod
    def parse_fights(response):

        fights = []
        fight_rows = response.css('ul[data-event-view-toggle-target="list"] > li[data-controller="table-row-background"]')

        for fight in fight_rows:
            fight_data = CardParser.parse_single_fight(fight, response)
            fights.append(fight_data)

        return fights

    @staticmethod
    def parse_single_fight(fight, response):
        fight_relative_url = fight.css('span.text-xs11 a::attr(href)').get()
        fight_id = URLUtils.extract_fight_id(fight_relative_url)

        # Dövüşçü bilgilerini al
        fighter1 = fight.css('div#f0smNameContainer a.link-primary-red::text').get(default='').strip()
        fighter2 = fight.css('div#f1smNameContainer a.link-primary-red::text').get(default='').strip()
        
        fighter1_relative_url = fight.css('div#f0smNameContainer a.link-primary-red::attr(href)').get(default='').strip()
        fighter2_relative_url = fight.css('div#f1smNameContainer a.link-primary-red::attr(href)').get(default='').strip()
        
        fighter1_url = response.urljoin(fighter1_relative_url) if fighter1_relative_url else ''
        fighter2_url = response.urljoin(fighter2_relative_url) if fighter2_relative_url else ''
        
        fighter1_id = URLUtils.extract_fighter_id(fighter1_url) if fighter1_url else None
        fighter2_id = URLUtils.extract_fighter_id(fighter2_url) if fighter2_url else None
        
        fighter1_image_url = fight.css('div.relative.order-first img::attr(src)').get(default='').strip()
        fighter2_image_url = fight.css('div.relative.order-last img::attr(src)').get(default='').strip()
        
        weight_class = fight.css('span.bg-tap_darkgold::text').get(default='').strip()
        method = fight.css('span.uppercase::text').get(default='').strip()
        round_info = fight.css('span.text-xs11.md\:text-xs10.leading-relaxed::text').get(default='').strip()

        ages = fight.css('table#boutComparisonTable td.text-neutral-950::text').getall()
        ages = [age.strip() for age in ages if 'years' in age]
        fighter1_age_at_fight = ages[0] if len(ages) > 0 else ''
        fighter2_age_at_fight = ages[1] if len(ages) > 1 else ''

        fight_result = FightResult.determine_fight_result(fight)
        winner_id = None
        if fight_result == 'win':
            winner_id = fighter1_id

        return {
            'fight_id': fight_id,
            'fighter1': {
                'name': fighter1,
                'id': fighter1_id,
                'age_at_fight': fighter1_age_at_fight,
                'url': fighter1_url,
                'image_url': fighter1_image_url,
            },
            'fighter2': {
                'name': fighter2,
                'id': fighter2_id,
                'age_at_fight': fighter2_age_at_fight,
                'url': fighter2_url,
                'image_url': fighter2_image_url,
            },
            'fight_result': fight_result, 
            'winner_id': winner_id,
            'weight_class': weight_class,
            'method': method,
            'round_info': round_info,
        }

