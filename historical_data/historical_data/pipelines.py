# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import redis
from .common.util import kucoin_data_to_df
import pandas_ta as ta
import pandas as pd


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
        df['time'] = df['time'].astype(int)
        df['Trans amount'] = df['Trans amount'].astype(float)

        #remove the last candle
        df = df[:-1]

        #print df info
        # print(df.info())

        key = f'{item["symbol"]}:{item["time_frame"]}'

        if not item['first_time']:
            data = self.redis.get(key)
            data = data.decode('utf-8')
            redis_df = pd.read_json(data)
            # redis_df['time'] = redis_df['time'].dt.tz_localize('UTC')
            #add df to redis_df if time is not in redis_df
            df = pd.concat([redis_df, df], ignore_index=True)
            df = df.drop_duplicates(subset='time', keep='last')
            df = df.sort_values(by='time')
            df = df.reset_index(drop=True)
            df = df.to_json()
            self.redis.set(key, df)
        else:
            df = df.to_json()
            self.redis.set(key, df)
        return item

    
