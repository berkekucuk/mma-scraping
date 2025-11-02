# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class UfcScraperItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass

class EventItem(scrapy.Item):
    event_id = scrapy.Field()  # PK
    event_url = scrapy.Field()
    event_type = scrapy.Field()
    event_name = scrapy.Field()
    date_time = scrapy.Field()
    venue = scrapy.Field()
    location = scrapy.Field()


class FightItem(scrapy.Item):
    fight_id = scrapy.Field()          # PK
    event_id = scrapy.Field()          # FK -> EventItem
    method_type = scrapy.Field()
    method_detail = scrapy.Field()
    round_summary = scrapy.Field()
    card_section = scrapy.Field()
    weight_class_lbs = scrapy.Field()
    rounds_format = scrapy.Field()
    card_number = scrapy.Field()


class FighterItem(scrapy.Item):
    fighter_id = scrapy.Field()        # PK
    name = scrapy.Field()
    nickname = scrapy.Field()
    record = scrapy.Field()
    date_of_birth = scrapy.Field()
    born = scrapy.Field()
    fighting_out_of = scrapy.Field()
    height = scrapy.Field()
    weight_class = scrapy.Field()
    reach = scrapy.Field()
    profile_url = scrapy.Field()
    image_url = scrapy.Field()


class FightParticipationItem(scrapy.Item):
    fight_id = scrapy.Field()          # FK -> FightItem
    fighter_id = scrapy.Field()        # FK -> FighterItem
    odds = scrapy.Field()
    age_at_fight = scrapy.Field()
    fight_result = scrapy.Field()


