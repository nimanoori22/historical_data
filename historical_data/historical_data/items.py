# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

class HistoricalDataItem(scrapy.Item):
    # define the fields for your item here like:
    symbol = scrapy.Field()
    time_frame = scrapy.Field()
    candles = scrapy.Field()
    first_time = scrapy.Field()


class SymbolsListItem(scrapy.Item):
    key = scrapy.Field()
    symbols = scrapy.Field()