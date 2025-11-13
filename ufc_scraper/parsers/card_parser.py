from urllib import response
from ..items import EventItem
from ..utils.datetime_parser import DatetimeParser
from ..utils.status_parser import StatusParser

class CardParser:

    @staticmethod
    def parse_card(response, event_id, event_url, spider):

        spider.logger.info(f"Parsing event: {event_id}")

        # --- Status ---
        status_string = response.css("div#eventPageHeader span::text").get(default="").strip()
        status = StatusParser.parse(status_string)

        # --- Event Name ---
        name = response.css("h2::text").get(default="").strip()

        # --- Main container ---
        container = response.css('ul[data-controller="unordered-list-background"]')

        # --- Date/Time ---
        date_time_str = container.xpath(".//span[contains(text(), 'Date/Time')]/following-sibling::span/text()").get(default="").strip()
        datetime_utc = DatetimeParser.parse_tapology_datetime(date_time_str)

        # --- Venue ---
        venue = container.xpath(".//span[contains(text(), 'Venue')]/following-sibling::span/text()").get(default="").strip()

        # --- Location ---
        location = container.xpath(".//span[contains(text(), 'Location')]/following-sibling::span//text()").get(default="").strip()

        # --- Create item ---
        event_item = EventItem()
        event_item["item_type"] = "event"
        event_item["event_id"] = event_id
        event_item["event_url"] = event_url
        event_item["name"] = name
        event_item["status"] = status
        event_item["datetime_utc"] = datetime_utc
        event_item["venue"] = venue
        event_item["location"] = location

        yield event_item
