# third party modules
import numpy as np
import pandas as pd
from datetime import timedelta as td
from collections import deque
import logging

from sklearn.preprocessing import MinMaxScaler
from sklearn.neural_network import MLPRegressor
from sklearn.pipeline import make_pipeline
from skforecast.ForecasterAutoreg import ForecasterAutoreg

# model modules
from demandlib.electric_profile import get_holidays

log = logging.getLogger('price_forecast')

# default_coal = 65.18 / 8.141 / 1e3  # €/ske --> €/MWh -> €/kWh
# default_lignite = 1.5 / 1e3  # agora Deutsche "Braunkohlewirtschaft" in €/kWh
# default_nuclear = 1 / 1e3  # no valid source but less then lignite [€/kWh]
# default_prices = pd.DataFrame()
# default_prices['gas']=default_gas # month mean values year 2018
# default_prices['co']=default_emission # month mean emission values per ton year 2018
# default_prices['coal']=[default_coal] *12
# default_prices['lignite']=[default_lignite] *12
# default_prices['nuclear']=[default_nuclear] *12
# default_prices.index.name = 'Month'
# default_prices.to_csv('./forecasts/data/monthly_generation_cost.csv')

# monthly prices
default_prices = pd.read_csv('./forecasts/data/monthly_generation_cost.csv')

# daily
# hourly_prices = pd.DataFrame()
# hourly_prices['power']=default_power_price # hourly mean values 2015-2018
# hourly_prices['demand']=default_demand # historic demand
# hourly_prices.index.name='hour'
# hourly_prices.to_csv('./forecasts/data/hourly_prices.csv')

hourly_prices = pd.read_csv('./forecasts/data/hourly_prices.csv')
default_power_price = hourly_prices['power']

exog = pd.read_pickle(r'./forecasts/data/historic_exog.pkl')
exog['demand'] = exog['demand'] / exog['demand'].max()
y = pd.read_pickle(r'./forecasts/data/historic_y.pkl') / 1e3    # prices in €/MWh elek -> convert to €/kWh

real_prices = pd.read_csv(r'./forecasts/data/history_price_15_22.csv', sep=',', decimal='.', parse_dates=True,
                          index_col=0)
real_prices.columns = ['price']
real_prices /= 1e3


class PriceForecast:

    def __init__(self, use_historic_data: bool = True, starting_date: pd.Timestamp = pd.Timestamp(2018, 1, 1),
                 use_real_data: bool = False):

        # initialize neural network and corresponding scaler
        model = MLPRegressor(hidden_layer_sizes=(15, 15,), activation='identity', early_stopping=True,
                             solver='adam', learning_rate_init=0.02, shuffle=True, max_iter=500)

        self.model = ForecasterAutoreg(regressor=make_pipeline(MinMaxScaler(), model),
                                       lags=[1, 2, 23, 24, 25, 48])

        self.last_window = pd.Series(dtype=float)
        self.last_price = pd.Series(dtype=float)
        self.input = deque(maxlen=400)
        self.output = deque(maxlen=400)
        self.fitted = False
        self._historic_range = []
        self.use_real_prices = use_real_data

        if use_historic_data:
            np.unique(exog.index.date)
            for date in np.unique(exog.index.date):
                if date < starting_date.date():
                    self.input.append(exog.loc[exog.index.date == date])
                    self.output.append(y.loc[y.index.date == date, 'price'])
                    self._historic_range.append(date)

    def _get_dummies(self, date: pd.Timestamp):
        d_range = pd.date_range(start=date - td(days=200), end=date + td(days=200), freq='h')
        # hourly flag (hour_0, ..., hour_23)
        hours = pd.get_dummies(d_range.hour)
        hours.columns = [f'hour_{h}' for h in hours.columns]
        # weekday flag (Monday, ..., Sunday)
        week_days = pd.get_dummies(d_range.day_name())
        # monthly flag (January, ..., December)
        month_of_year = pd.get_dummies(d_range.month_name())
        # weekly flag (week_0, ..., week_51)
        week_of_year = pd.get_dummies(d_range.isocalendar().week)
        week_of_year.columns = [f'week_{c}' for c in week_of_year.columns]
        week_of_year.index = range(len(week_of_year))
        years = d_range.year.unique()
        dummies = pd.concat([hours, week_days, month_of_year, week_of_year], axis=1)
        dummies['holiday'] = 0
        dummies.index = d_range
        for year in years:
            holidays = get_holidays(year=year)
            for day in holidays:
                dummies.loc[(dummies.index.day_of_year == day) & (dummies.index.year == year), 'holiday'] = 1

        return dummies.loc[dummies.index.day_of_year == date.day_of_year, :]

    def collect_data(self, date: pd.Timestamp, market_result: pd.DataFrame, weather: pd.DataFrame):
        data = {column: weather[column].values.flatten() for column in weather.columns}
        if len(market_result) != len(weather):
            log.warning('market results are not valid, set default demand and default price.')
            data['demand'] = default_demand
            price = hourly_prices['power'].copy()
            price.index = pd.date_range(start=date, freq='h', periods=len(df))
        else:
            data['demand'] = market_result['volume'].values.flatten()
            price = market_result['price']

        data = pd.DataFrame(data, index=pd.date_range(start=date, freq='h', periods=len(weather)))
        dummies = self._get_dummies(date)

        self.input.append(pd.concat([data, dummies], axis=1))
        self.output.append(price)

        if len(self.last_price) > 0:
            self.last_window = pd.concat([self.last_price, market_result['price']], axis=0)
        self.last_price = price

    def fit_model(self):
        input = pd.concat(self.input, axis=0).asfreq('h')
        if len(self._historic_range) > 0:
            index = input.index.isin(self._historic_range)
            input.loc[index, 'demand'] *= input.loc[~index, 'demand'].max()

        self.model.fit(y=pd.concat(self.output, axis=0).asfreq('h'),
                       exog=input)

        self.fitted = True

    def forecast(self, date: pd.Timestamp, demand: pd.Series, weather: pd.DataFrame, steps: int = 24):
        if (steps % 24) > 0:
            print(f'wrong step size: {steps}')
            steps -= steps % 24
            print(f'set step size to {steps}')
        steps = max(steps, 24)
        range_ = pd.date_range(start=date, end=date + td(days=int(steps / 24) - 1), freq='d')

        def noise(x):
            return np.random.uniform(0.95, 1.05, x)

        if not self.fitted and not self.use_real_prices:
            power_price = np.repeat(default_power_price, int(steps / 24)) * noise(steps)
        elif self.use_real_prices:
            power_price = pd.concat([real_prices.loc[real_prices.index.date == d.date()] for d in range_], axis=0)
            power_price = power_price.values.flatten()
        else:
            data = {column: weather[column].values.flatten() for column in weather.columns}
            data['demand'] = demand.values.flatten()
            data = pd.DataFrame(data, index=pd.date_range(start=date, freq='h', periods=len(weather)))
            dummies = pd.concat([self._get_dummies(d) for d in range_], axis=0)
            data = pd.concat([data, dummies], axis=1)
            power_price = self.model.predict(steps=steps, last_window=self.last_window, exog=data).values

        def make_day_price(price):
            return pd.Series(price * (np.ones_like(default_power_price) * noise(steps)))
        m = date.month-1
        prices = default_prices.iloc[m, :].apply(make_day_price).T
        prices.index = pd.date_range(start=date, periods=steps, freq='h')
        prices.index.name = 'time'
        return prices


if __name__ == "__main__":
    pf = PriceForecast()
    pf.collect_data('2018-01-01')
    pf.fit_model()
    forecast = pf.forecast('2018-01-02')
