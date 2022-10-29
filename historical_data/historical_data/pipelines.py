# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import redis
from .common.util import kucoin_data_to_df
import pandas_ta as ta


class SymbolsListPipeline:

    def open_spider(self, spider):
        self.redis = redis.Redis(host='localhost', port=6379, db=0)

    def close_spider(self, spider):
        self.redis.close()

    def process_item(self, item, spider):
        symbols = item['symbols']
        self.redis.set('symbols', symbols)
        return item


class RedisPipeline:

    def open_spider(self, spider):
        self.redis = redis.Redis(host='localhost', port=6379, db=0)

    def close_spider(self, spider):
        self.redis.close()

    
    def process_item(self, item, spider):
        df = kucoin_data_to_df(item['candles'])

        df['open'] = df['open'].astype(float)
        df['high'] = df['high'].astype(float)
        df['low'] = df['low'].astype(float)
        df['close'] = df['close'].astype(float)
        df['volume'] = df['volume'].astype(float)
        df['Trans amount'] = df['Trans amount'].astype(float)

        supertrend = ta.supertrend(df['high'], df['low'], df['close'], length=10, multiplier=3)
        ema_50 = ta.ema(df['close'], length=50)
        ema_200 = ta.ema(df['close'], length=200)

        df['SUPERT_10_3.0'] = supertrend['SUPERT_10_3.0']
        df['SUPERTs_10_3.0'] = supertrend['SUPERTs_10_3.0']
        df['SUPERTd_10_3.0'] = supertrend['SUPERTd_10_3.0']
        df['SUPERTl_10_3.0'] = supertrend['SUPERTl_10_3.0']
        df['EMA_50'] = ema_50
        df['EMA_200'] = ema_200

        # calculate the difference between the ema_50 and ema_200 in percentage
        df['EMA_DIFF'] = abs(ema_50 - ema_200) / ((ema_50 + ema_200) / 2) * 100
        
        key = f'{item["symbol"]}:{item["time_frame"]}'
        self.redis.set(key, df.to_json())
        return item

    
