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
