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
    event_id = scrapy.Field()          # PK
    event_type = scrapy.Field()
    event_name = scrapy.Field()
    date_time = scrapy.Field()
    venue = scrapy.Field()
    location = scrapy.Field()


class FightItem(scrapy.Item):
    fight_id = scrapy.Field()          # PK
    event_id = scrapy.Field()          # FK -> EventItem
    weight_class = scrapy.Field()
    method = scrapy.Field()
    round_info = scrapy.Field()
    fight_result = scrapy.Field()
    winner_id = scrapy.Field()         # FK -> FighterItem


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
    corner = scrapy.Field()
    odds = scrapy.Field()           # "Red" veya "Blue" (ya da fighter1/fighter2)
    age_at_fight = scrapy.Field()


