from ..utils.date_time_util import DateTimeUtil

class CardParser:

    @staticmethod
    def parse_card(response):
        status = response.css('div#eventPageHeader span::text').get(default='').strip()

        name = response.css('h2::text').get(default='').strip()

        container = response.css('ul[data-controller="unordered-list-background"]')

        date_time_str = container.xpath("//span[contains(text(), 'Date/Time')]/following-sibling::span/text()").get(default='').strip()
        datetime_utc = DateTimeUtil.parse_tapology_datetime(date_time_str)

        venue = container.xpath("//span[contains(text(), 'Venue')]/following-sibling::span/text()").get(default='').strip()

        location = container.xpath("//span[contains(text(), 'Location')]/following-sibling::span//text()").get(default='').strip()

        return {
            'status': status,
            'name': name,
            'datetime_utc': datetime_utc,
            'venue': venue,
            'location': location,
        }
