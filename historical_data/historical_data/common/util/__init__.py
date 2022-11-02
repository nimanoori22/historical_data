import pandas as pd

def kucoin_data_to_df(historical_data):
    data = pd.DataFrame(historical_data)
    data.columns = ['time', 'open', 'close', 'high', 'low', 'volume', 'Trans amount']
    data = data.sort_values(by='time', ascending=True)
    data = data.reset_index(drop=True)
    return data