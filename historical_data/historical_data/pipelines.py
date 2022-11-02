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

        key = f'{item["symbol"]}:{item["time_frame"]}'

        if not item['first_time']:
            data = self.redis.get(key)
            data = data.decode('utf-8')
            redis_df = pd.read_json(data)
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


class TADataPipeline:

    def open_spider(self, spider):
        self.redis = redis.Redis(host='localhost', port=6379, db=0)

    def close_spider(self, spider):
        self.redis.close()

    def process_item(self, item, spider):
        #get data from redis
        key = f'{item["symbol"]}:{item["time_frame"]}'
        data = self.redis.get(key)
        data = data.decode('utf-8')
        df = pd.read_json(data)

        if 'SUPERT_10_3.0' not in df.columns:
            supertrend = ta.supertrend(
                df['high'], df['low'], df['close'], length=10, multiplier=3
            )
            df['SUPERT_10_3.0'] = supertrend['SUPERT_10_3.0']
            df['SUPERTs_10_3.0'] = supertrend['SUPERTs_10_3.0']
            df['SUPERTd_10_3.0'] = supertrend['SUPERTd_10_3.0']
            df['SUPERTl_10_3.0'] = supertrend['SUPERTl_10_3.0']
        else:
            #calculate supertrend for the last 300 candles and update them
            supertrend = ta.supertrend(
                df['high'][-300:], 
                df['low'][-300:], 
                df['close'][-300:], 
                length=10, multiplier=3
            )
            df['SUPERT_10_3.0'][-300:] = supertrend['SUPERT_10_3.0']
            df['SUPERTs_10_3.0'][-300:] = supertrend['SUPERTs_10_3.0']
            df['SUPERTd_10_3.0'][-300:] = supertrend['SUPERTd_10_3.0']
            df['SUPERTl_10_3.0'][-300:] = supertrend['SUPERTl_10_3.0']
        
        if 'EMA_50' not in df.columns:
            ema_50 = ta.ema(df['close'], length=50)
            ema_200 = ta.ema(df['close'], length=200)
            df['EMA_50'] = ema_50
            df['EMA_200'] = ema_200
        else:
            ema_50 = ta.ema(df['close'][-300:], length=50)
            ema_200 = ta.ema(df['close'][-300:], length=200)
            df['EMA_50'][-300:] = ema_50
            df['EMA_200'][-300:] = ema_200
        
        if 'EMA_DIFF' not in df.columns:
            # calculate the difference between the ema_50 and ema_200 in percentage
            df['EMA_DIFF'] = abs(ema_50 - ema_200) / ((ema_50 + ema_200) / 2) * 100
        else:
            last_300_ema_50 = ta.ema(df['close'][-300:], length=50)
            last_300_ema_200 = ta.ema(df['close'][-300:], length=200)
            df['EMA_DIFF'][-300:] = abs(last_300_ema_50 - last_300_ema_200) / ((last_300_ema_50 + last_300_ema_200) / 2) * 100

        self.redis.set(key, df.to_json())
        return item
