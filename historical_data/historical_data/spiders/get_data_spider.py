import scrapy
from kucoin.client import Market
import pandas as pd
import time


class GetSimbolsSpider(scrapy.Spider):
    name = 'get_symbols'
    allowed_domains = ['kucoin.com']
    start_urls = ['https://www.kucoin.com/']

    def parse(self, response):
        client = Market(url='https://api.kucoin.com')
        symbols = client.get_symbol_list()
        df = pd.DataFrame(symbols)
        # filter out the symbols that have 3l or 3s in them
        df = df[~df['symbol'].str.contains('3L|3S')]
        #filter out rows with no usdt as the quote currency
        df = df[df['quoteCurrency'] == 'USDT']
        #reset index
        df = df.reset_index(drop=True)
        print(df)


class GetDataSpider(scrapy.Spider):
    name = 'get_data'
    allowed_domains = ['https://api.kucoin.com/']

    def start_requests(self):

        market = Market(url='https://api.kucoin.com')
        symbols = market.get_symbol_list()
        # print(type(symbols))
        df = pd.DataFrame(symbols)
        # filter out the symbols that have 3l or 3s in them
        df = df[~df['symbol'].str.contains('3L|3S')]
        #filter out rows with no usdt as the quote currency
        df = df[df['quoteCurrency'] == 'USDT']
        #reset index
        df = df.reset_index(drop=True)

        # df to 20 length lists of symbols
        df = df['symbol'].tolist()

        # get time of now without decimals
        now = int(time.time())
        # now minus 300 5min intervals
        start = now - (300 * 5 * 60)

        #get 10 first symbols in df
        my_symbols = df

        urls = [
            f'https://api.kucoin.com/api/v1/market/\
                candles?type=5min&symbol={symbol}&startAt={start}&endAt={now}'
            for symbol in my_symbols
        ]

        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        print(response.json())
