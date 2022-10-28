import pandas as pd

def kucoin_data_to_df(historical_data):
    data = pd.DataFrame(historical_data)
    data.columns = ['time', 'open', 'close', 'high', 'low', 'volume', 'Trans amount']
    data['time'] = pd.to_datetime(data['time'], unit='s')
    data.set_index('time', inplace=True)
    data.sort_index(inplace=True)
    # data.index = data.index.tz_localize('UTC').tz_convert('Asia/Tehran')
    data.index = data.index.tz_localize('UTC')
    return data