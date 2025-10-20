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
    event_id = scrapy.Field()
    event_type = scrapy.Field()
    event_name = scrapy.Field()
    date_time = scrapy.Field()
    venue = scrapy.Field()
    location = scrapy.Field()
    fights = scrapy.Field()

class FightItem(scrapy.Item):
    fight_id = scrapy.Field()
    fighter1 = scrapy.Field()
    fighter2 = scrapy.Field()
    fight_result = scrapy.Field()
    winner_id = scrapy.Field()
    weight_class = scrapy.Field()
    method = scrapy.Field()
    round_info = scrapy.Field()

class FighterItem(scrapy.Item):
    fighter_id = scrapy.Field()
    name = scrapy.Field()
    nickname = scrapy.Field()
    record = scrapy.Field()
    date_of_birth = scrapy.Field()
    born = scrapy.Field()
    fighting_out_of = scrapy.Field()
    height = scrapy.Field()
    weight_class = scrapy.Field()
    reach = scrapy.Field()
    image_url = scrapy.Field()




