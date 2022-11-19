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
from .common.util.indicators import rainbow_adaptive_rsi, is_consolidating
import numpy as np


class SymbolsListPipeline:

    def open_spider(self, spider):
        self.redis = redis.Redis(host='localhost', port=6379, db=0)

    def close_spider(self, spider):
        self.redis.close()

    def process_item(self, item, spider):
        key = item['key']
        symbols = item['symbols']
        self.redis.set(key, symbols)
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
                df.loc[-20:, 'high'], 
                df.loc[-20:, 'low'], 
                df.loc[-20:, 'close'], 
                length=10, multiplier=3
            )
            df.loc[-20:, 'SUPERT_10_3.0'] = supertrend['SUPERT_10_3.0']
            df.loc[-20:, 'SUPERTs_10_3.0'] = supertrend['SUPERTs_10_3.0']
            df.loc[-20:, 'SUPERTd_10_3.0'] = supertrend['SUPERTd_10_3.0']
            df.loc[-20:, 'SUPERTl_10_3.0'] = supertrend['SUPERTl_10_3.0']
        
        if 'EMA_50' not in df.columns:
            ema_50 = ta.ema(df['close'], length=50)
            ema_200 = ta.ema(df['close'], length=200)
            df['EMA_50'] = ema_50
            df['EMA_200'] = ema_200
        else:
            ema_50 = ta.ema(df.loc[-300:, 'close'], length=50)
            ema_200 = ta.ema(df.loc[-400:, 'close'], length=200)
            df.loc[-300:, 'EMA_50'] = ema_50
            df.loc[-400:, 'EMA_200'] = ema_200
        
        if 'EMA_DIFF' not in df.columns:
            # calculate the difference between the ema_50 and ema_200 in percentage
            df['EMA_DIFF'] = abs(ema_50 - ema_200) / ((ema_50 + ema_200) / 2) * 100
        else:
            last_300_ema_50 = ta.ema(df.loc[-300:, 'close'], length=50)
            last_300_ema_200 = ta.ema(df.loc[-300:, 'close'], length=200)
            df.loc[-300:, 'EMA_DIFF'] = abs(last_300_ema_50 - last_300_ema_200) / ((last_300_ema_50 + last_300_ema_200) / 2) * 100
        
        if 'trigger' not in df.columns:
            rainbow_rsi = rainbow_adaptive_rsi(df['close'])
            df['trigger'] = rainbow_rsi['trigger']
            df['rsi'] = rainbow_rsi['rsi']
        else:
            rainbow_rsi = rainbow_adaptive_rsi(df.loc[-50:, 'close'])
            df.loc[-50:, 'trigger'] = rainbow_rsi.loc[-50:, 'trigger']
            df.loc[-20:, 'rsi'] = rainbow_rsi.loc[-20:, 'rsi']
        
        # if SUPERTd_10_3.0 is 1 and previous SUPERTd_10_3.0 is -1 set buy signal to 1
        df['buy_signal'] = np.where(
            (df['SUPERTd_10_3.0'] == 1) & (df['SUPERTd_10_3.0'].shift(1) == -1), 1, 0
        )
        # if SUPERTd_10_3.0 is -1 and previous SUPERTd_10_3.0 is 1 set sell signal to 1
        df['sell_signal'] = np.where(
            (df['SUPERTd_10_3.0'] == -1) & (df['SUPERTd_10_3.0'].shift(1) == 1), 1, 0
        )

        # average of rsi nad trigger
        if 'avg_trigger_rsi' not in df.columns:
            df['avg_trigger_rsi'] = (df['trigger'] + df['rsi']) / 2
        else:
            df.loc[-50:, 'avg_trigger_rsi'] = (df.loc[-50:, 'trigger'] \
                + df.loc[-50:, 'rsi']) / 2
        
        df['is_consolidating'] = is_consolidating(df, 5)
        

        self.redis.set(key, df.to_json())
        return item
