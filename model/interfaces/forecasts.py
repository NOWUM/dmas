import numpy as np

from apps.frcst_Dem import typFrcst as demandForecast
from apps.frcst_Price import annFrcst as priceForecast
from forecasts.weather import weatherForecast
from apps.misc_WeekPriceList import WeekPriceList


class Forecasts:

    def __init__(self):
        self.default_prices = [37.70, 35.30, 33.90, 33.01, 33.27, 35.78, 43.17, 50.21, 52.89, 51.18, 48.24, 46.72,
                               44.23, 42.29, 41.60, 43.12, 45.37, 50.95, 55.12, 56.34, 52.70, 48.20, 45.69, 40.25]
        self.week_price_list = WeekPriceList(defaultDay=np.asarray(self.default_prices))

        # forecast methods
        self.forecasts = {
            'demand': demandForecast(),
            'weather': weatherForecast(),
            'price': priceForecast()
        }

    def weather_forecast(self, date=pd.to_datetime('2019-01-01'), days=1, mean=False):
        weather = dict(wind=[], dir=[], dif=[], temp=[])
        for i in range(days):
            # get weather data for day (i)
            w = self.forecasts['weather'].forecast(str(self.geo), date + pd.DateOffset(days=i), mean)
            for key, value in w.items():
                weather[key] = np.concatenate((weather[key], value * np.random.uniform(0.95, 1.05, 24)))
        return weather


def price_forecast(self, date=pd.to_datetime('2019-01-01'), days=1):
    # max forecast period is one week! --> Error
    price = dict(power=[], gas=[], co=[], lignite=3.5, coal=8.5, nuc=1)
    start = pd.to_datetime(date)
    end = pd.to_datetime(date) + pd.DateOffset(days=days - 1)

    last_forecast = None
    counter = 0
    for d in pd.date_range(start=start, end=end, freq='d'):
        demand = self.forecasts['demand'].forecast(d)
        weather = self.forecasts['weather'].forecast(str(self.geo), d, mean=True)

        if last_forecast is None:
            price_d1 = self.week_price_list.get_price_yesterday()
            price_d7 = self.week_price_list.get_price_week_before()
        else:
            price_d1 = last_forecast
            price_d7 = self.week_price_list.get_price_x_days_before(max(7 - counter, 1))

        p = self.forecasts['price'].forecast(d, demand, weather, price_d1, price_d7)

        last_forecast = p['power']
        counter += 1
        for key, value in p.items():
            if key in ['power', 'gas', 'co']:
                price[key] = np.round(np.concatenate((price[key], value)), 2)
            else:
                price[key] = np.round(value, 2)
    return price


def demand_forecast(self, date=pd.to_datetime('2019-01-01'), days=1):
    demand = []
    for i in range(days):
        demand += list(self.forecasts['demand'].forecast(date))
    return np.asarray(demand).reshape((-1,))