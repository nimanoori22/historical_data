import pandas as pd
import pandas_ta as ta

def rainbow_adaptive_rsi(
    close: pd.Series, 
    length: int = 15,
    ama_length: int = 10) -> pd.DataFrame:
    """
    Rainbow Adaptive RSI
    https://www.tradingview.com/script/ZIO3hXCd-Rainbow-Adaptive-RSI-LUX/
    :param close: (pd.Series) Close prices
    :param length: (int) RSI period. Default: 15
    :param ama_length: (int) adaptive moving average period. Default: 10
    :return: (pd.DataFrame) RSI
    """
    kama = ta.kama(close, ama_length)
    rsi = ta.rsi(kama, length)
    trigger = ta.ema(ta.rsi(ta.ema(close, length/2), length), length/2)
    return pd.DataFrame({'rsi': rsi, 'trigger': trigger})


def is_consolidating(df, percentage=2):
    recent_candlesticks = df[-15:]
    
    max_close = recent_candlesticks['close'].max()
    min_close = recent_candlesticks['close'].min()

    threshold = 1 - (percentage / 100)
    if min_close > (max_close * threshold):
        return True        

    return False


def is_breaking_out(df, percentage=2.5):
    last_close = df[-1:]['close'].values[0]

    if is_consolidating(df[:-1], percentage=percentage):
        recent_closes = df[-16:-1]

        if last_close > recent_closes['close'].max():
            return True

    return False
