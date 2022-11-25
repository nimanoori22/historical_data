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
from .common.util.indicators import is_consolidating
import numpy as np
import json


class SymbolsListPipeline:

    def open_spider(self, spider):
        self.redis = redis.Redis(host='localhost', port=6379, db=0)

    def close_spider(self, spider):
        self.redis.close()

    def process_item(self, item, spider):
        key = item['key']
        symbols = item['symbols']
        self.redis.set(key, symbols)

        if key == 'top_100':
            symbols = self.redis.get('symbols')
            symbols = symbols.decode('utf-8')
            symbols = json.loads(symbols)
            top_100 = self.redis.get('top_100')
            top_100 = top_100.decode('utf-8')
            top_100 = json.loads(top_100)

            top_100_symbols = set(map(lambda x: x['symbol'], top_100))
            #delete -usdt from every symbol in symbols
            symbols = list(map(lambda x: x[:-5].lower(), symbols))
            symbols = list(filter(lambda x: x in top_100_symbols, symbols))
            # add -usdt to every symbol in symbols
            symbols = list(map(lambda x: x.upper() + '-USDT', symbols))
            print(symbols, '-'*50)

            symbols = json.dumps(symbols)
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
        if item['first_time']:
            return item
        #get data from redis
        key = f'{item["symbol"]}:{item["time_frame"]}'
        data = self.redis.get(key)
        data = data.decode('utf-8')
        df = pd.read_json(data)
        
        slice_df_20 = df[-20:]

        supertrend = ta.supertrend(
            slice_df_20['high'], 
            slice_df_20['low'], 
            slice_df_20['close'], 
            length=10, multiplier=3
        )
        # print(supertrend, '-'*50)
        if 'SUPERT_10_3.0' not in df.columns:
            # create new columns
            df['SUPERT_10_3.0'] = np.nan
            df['SUPERTs_10_3.0'] = np.nan
            df['SUPERTd_10_3.0'] = np.nan
            df['SUPERTl_10_3.0'] = np.nan
        
        df['SUPERT_10_3.0'][-20:] = supertrend['SUPERT_10_3.0']
        df['SUPERTs_10_3.0'][-20:] = supertrend['SUPERTs_10_3.0']
        df['SUPERTd_10_3.0'][-20:] = supertrend['SUPERTd_10_3.0']
        df['SUPERTl_10_3.0'][-20:] = supertrend['SUPERTl_10_3.0']

        if 'EMA_50' not in df.columns:
            # create new columns
            df['EMA_50'] = np.nan
        if 'EMA_200' not in df.columns:
            # create new columns
            df['EMA_200'] = np.nan
        
        slice_df_210 = df[-210:]

        ema_50 = ta.ema(slice_df_210.iloc[-60:]['close'], length=50)
        ema_200 = ta.ema(slice_df_210.iloc[-210:]['close'], length=200)
        len_ema_50 = len(ema_50)
        len_ema_200 = len(ema_200)
        df['EMA_50'][-len_ema_50:] = ema_50
        df['EMA_200'][-len_ema_200:] = ema_200
        
        if 'EMA_DIFF' not in df.columns:
            df['EMA_DIFF'] = np.nan
        
        if 'BUY_SIGNAL' not in df.columns:
            df['BUY_SIGNAL'] = np.nan
        if 'SELL_SIGNAL' not in df.columns:
            df['SELL_SIGNAL'] = np.nan
        
        slice_df_2 = df[-2:]
        
        last_ema_50 = slice_df_2.iloc[-1]['EMA_50']
        last_ema_200 = slice_df_2.iloc[-1]['EMA_200']

        ema_diff = abs(last_ema_50 - last_ema_200) / \
            ((last_ema_50 + last_ema_200) / 2) * 100
        
        df['EMA_DIFF'].iloc[-1] = ema_diff
        
        if slice_df_2['SUPERTd_10_3.0'].iloc[-1] == 1 \
            and slice_df_2['SUPERTd_10_3.0'].iloc[-2] == -1:
            df['BUY_SIGNAL'].iloc[-1] = 1
        else:
            df['BUY_SIGNAL'].iloc[-1] = 0

        if slice_df_2['SUPERTd_10_3.0'].iloc[-1] == -1 \
            and slice_df_2['SUPERTd_10_3.0'].iloc[-1] == 1:
            df['SELL_SIGNAL'].iloc[-1] = 1
        else:
            df['SELL_SIGNAL'].iloc[-1] = 0
        
        if 'is_consolidating' not in df.columns:
            df['is_consolidating'] = np.nan
        
        df['is_consolidating'].iloc[-1] = is_consolidating(slice_df_20, 5)
        

        self.redis.set(key, df.to_json())
        return item
