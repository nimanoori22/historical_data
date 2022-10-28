import scrapy
from kucoin.client import Market
import pandas as pd
import time
import json
from historical_data.items import HistoricalDataItem, SymbolsListItem
import redis

class GetSimbolsSpider(scrapy.Spider):
    name = 'get_symbols'
    allowed_domains = ['kucoin.com']
    start_urls = ['https://www.kucoin.com/']

    custom_settings = {
        'ITEM_PIPELINES': {
            'historical_data.pipelines.SymbolsListPipeline': 300
        }
    }

    def parse(self, response):
        client = Market(url='https://api.kucoin.com')
        symbols = client.get_symbol_list()
        df = pd.DataFrame(symbols)
        # filter out the symbols that have 3l or 3s in them
        df = df[~df['symbol'].str.contains('3L|3S')]
        #filter out rows with no usdt as the quote currency
        df = df[df['quoteCurrency'] == 'USDT']
        df = df.reset_index(drop=True)
        df = df['symbol'].tolist()
        symbols = json.dumps(df)
        symbols_item = SymbolsListItem()
        symbols_item['symbols'] = symbols
        yield symbols_item


class GetDataSpider(scrapy.Spider):
    name = 'get_data'
    allowed_domains = ['https://api.kucoin.com/']

    custom_settings = {
        'ITEM_PIPELINES': {
            'historical_data.pipelines.RedisPipeline': 400
        }
    }

    def start_requests(self):
        
        self.redis = redis.Redis(host='localhost', port=6379, db=0)
        symbols = self.redis.get('symbols')
        self.redis.close()
        symbols = json.loads(symbols)

        # get time of now without decimals
        now = int(time.time())
        # now minus 300 5min intervals
        start = now - (300 * 5 * 60)
        
        my_symbols = symbols

        urls = [
            f'https://api.kucoin.com/api/v1/market/candles?type=5min&symbol={symbol}&startAt={start}&endAt={now}'
            for symbol in my_symbols
        ]

        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        url = response.url
        symbol = url.split('=')[2].split('&')[0]
        time_frame = url.split('=')[1].split('&')[0]
        data = response.json()
        historical_data_item = HistoricalDataItem()
        historical_data_item['symbol'] = symbol
        historical_data_item['time_frame'] = time_frame
        historical_data_item['candles'] = data['data']
        yield historical_data_item
