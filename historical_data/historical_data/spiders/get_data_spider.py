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
        symbols_item['key'] = 'symbols'
        symbols_item['symbols'] = symbols
        yield symbols_item


class GetDataSpider(scrapy.Spider):
    name = 'get_data'
    allowed_domains = ['kucoin.com']

    custom_settings = {
        'ITEM_PIPELINES': {
            'historical_data.pipelines.RedisPipeline': 400,
            'historical_data.pipelines.TADataPipeline': 500
        }
    }

    def start_requests(self):
        
        self.redis = redis.Redis(host='localhost', port=6379, db=0)
        symbols = self.redis.get('symbols')
        self.redis.close()
        symbols = json.loads(symbols)

        # get time of now without decimals
        now = int(time.time())
        # now minus 10 5min intervals
        # start = now - (1500 * 5 * 60)
        # now minus 1500 1hour intervals
        start = now - (1500 * 60 * 60)
        # time_frame = '5min'
        time_frame = '1hour'

        def get_start_time(symbol : str, time_frame : str) -> str:
            try:
                if self.redis.exists(f'{symbol}:{time_frame}'):
                    data = self.redis.get(f'{symbol}:{time_frame}')
                    data = data.decode('utf-8')
                    df = pd.read_json(data)
                    #get the second to last time
                    start_time = int(df['time'].iloc[-2])
                    return str(start_time)
                else:
                    return str(start)
            except Exception as e:
                print(e, '-------------------------------------')
                print(symbol)
                return str(start)



        base_url = 'https://api.kucoin.com/api/v1/market/candles'
        urls = [
            f'{base_url}?type=5min&symbol={symbol}&startAt={get_start_time(symbol, time_frame)}&endAt={now}'
            for symbol in symbols
        ]

        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        url = response.url
        symbol = url.split('=')[2].split('&')[0]
        time_frame = url.split('=')[1].split('&')[0]
        key = f'{symbol}:{time_frame}'
        
        historical_data_item = HistoricalDataItem()

        if self.redis.exists(key):
            data = self.redis.get(key)
            data = data.decode('utf-8')
            if len(data) > 0:
                historical_data_item['first_time'] = False
            else:
                historical_data_item['first_time'] = True
        else:
            historical_data_item['first_time'] = True
        
        historical_data_item['symbol'] = symbol
        historical_data_item['time_frame'] = time_frame
        data = response.json()
        historical_data_item['candles'] = data['data']
        yield historical_data_item


class GetTop100Spider(scrapy.Spider):
    
    """
    This spider is used to get the top 100 coins by market cap
    """

    name = 'get_top_100'
    allowed_domains = ['coingecko.com']
    start_urls = ['https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=100&page=1&sparkline=false']

    custom_settings = {
        'ITEM_PIPELINES': {
            'historical_data.pipelines.SymbolsListPipeline': 300
        }
    }

    def parse(self, response):
        data = response.json()
        symbols_item = SymbolsListItem()
        symbols_item['key'] = 'top_100'
        data = json.dumps(data)
        symbols_item['symbols'] = data
        yield symbols_item
