import pandas as pd
from matplotlib import pyplot as plt

def get_prices(date_range: pd.DatetimeIndex = None):
    if date_range is None:
        date_range = pd.date_range(start='2018-05-01', end='2018-08-01', freq='h')
    df = pd.read_csv(r'./forecasts/data/history_prices.csv', index_col=0, parse_dates=True, sep=';', decimal=',')
    return df.loc[date_range]


if __name__ == "__main__":
    prices = get_prices()
    smoothed = prices.ewm(com=24).mean()
    plt.plot(prices.values)
    plt.plot(smoothed.values)
    plt.show()
    # prices.plot()
    # smoothed.plot()