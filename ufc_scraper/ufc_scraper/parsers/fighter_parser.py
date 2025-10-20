from ..items import FighterItem

class FighterParser:

    @staticmethod
    def parse_fighter(response):
        
        name = response.xpath('//div[contains(@class, "text-tap_3")]/text()').get(default='').strip()
        container = response.css('div#standardDetails')

        fighter_data = {
            'name': name,
            'nickname': FighterParser.extract_detail(container, "Nickname:"),
            'record': FighterParser.extract_detail(container, "Pro MMA Record:"),
            'date_of_birth': FighterParser.extract_detail(container, "Date of Birth:"),
            'born': FighterParser.extract_detail(container, "Born:"),
            'fighting_out_of': FighterParser.extract_detail(container, "Fighting out of:"),
            'height': FighterParser.extract_detail(container, "Height:"),
            'weight_class': FighterParser.extract_detail(container, "Weight Class:"),
            'reach': FighterParser.extract_reach(response)
        }

        return fighter_data

    @staticmethod
    def extract_detail(container, label):
        return container.css(f'strong:contains("{label}") + span::text').get(default='').strip()

    @staticmethod
    def extract_reach(response):
        return response.xpath('//strong[contains(text(), "Reach")]/ancestor::div/following-sibling::div[1]/span/text()').get(default='').strip()