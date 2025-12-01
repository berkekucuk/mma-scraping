# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class EventItem(scrapy.Item):
    item_type = scrapy.Field()
    event_id = scrapy.Field()  # PK
    event_url = scrapy.Field()
    status = scrapy.Field()
    name = scrapy.Field()
    datetime_utc = scrapy.Field()
    venue = scrapy.Field()
    location = scrapy.Field()


class FightItem(scrapy.Item):
    item_type = scrapy.Field()
    fight_id = scrapy.Field()  # PK
    event_id = scrapy.Field()  # FK -> EventItem
    method_type = scrapy.Field()
    method_detail = scrapy.Field()
    round_summary = scrapy.Field()
    bout_type = scrapy.Field()
    weight_class_lbs = scrapy.Field()
    weight_class_id = scrapy.Field()
    rounds_format = scrapy.Field()
    fight_order = scrapy.Field()


class FighterItem(scrapy.Item):
    item_type = scrapy.Field()
    fighter_id = scrapy.Field()  # PK
    name = scrapy.Field()
    nickname = scrapy.Field()
    record = scrapy.Field()
    date_of_birth = scrapy.Field()
    height = scrapy.Field()
    reach = scrapy.Field()
    weight_class_id = scrapy.Field()
    born = scrapy.Field()
    fighting_out_of = scrapy.Field()
    style = scrapy.Field()
    country_code = scrapy.Field()
    profile_url = scrapy.Field()
    image_url = scrapy.Field()


class FightParticipationItem(scrapy.Item):
    item_type = scrapy.Field()
    fight_id = scrapy.Field()  # FK -> FightItem
    fighter_id = scrapy.Field()  # FK -> FighterItem
    odds_value = scrapy.Field()
    odds_label = scrapy.Field()
    result = scrapy.Field()
    record_after_fight = scrapy.Field()
    is_red_corner = scrapy.Field()
