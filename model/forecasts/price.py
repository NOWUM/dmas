# third party modules
import numpy as np
import pandas as pd

from forecasts.dummies import create_dummies
from sklearn.preprocessing import MinMaxScaler
from sklearn.neural_network import MLPRegressor
from collections import deque

# model modules
from forecasts.basic_forecast import BasicForecast
from forecasts.demand import DemandForecast
from forecasts.weather import WeatherForecast


with open(r'./forecasts/data/default_price.pkl', 'rb') as file:
    default_power_price = np.load(file).reshape((24,))                  # hourly mean values 2015-2018
with open(r'./forecasts/data/default_gas.pkl', 'rb') as file:
    default_gas = np.load(file).reshape((12,))                          # month mean values year 2018
with open(r'./forecasts/data/default_emission.pkl', 'rb') as file:
    default_emission = np.load(file).reshape((12,))                     # month mean values year 2018

default_coal = 65.18 / 8.141                                            # €/ske --> €/MWh
default_lignite = 1.5                                                   # agora Deutsche "Braunkohlewirtschaft"


class PriceForecast(BasicForecast):

    def __init__(self, position, simulation_interface, weather_interface):
        super().__init__(position, simulation_interface, weather_interface)

        # initialize neural network and corresponding scaler
        self.model = MLPRegressor(hidden_layer_sizes=(15, 15,), activation='identity', early_stopping=True,
                                  solver='adam', learning_rate_init=0.02, shuffle=True, max_iter=500)
        self.scale = MinMaxScaler()
        self.score = 0.

        self.price_register = deque(maxlen=8)
        for _ in range(8):
            self.price_register.append(default_power_price)

        self.demand_model = DemandForecast(self.position, simulation_interface, weather_interface)
        self.weather_model = WeatherForecast(self.position, simulation_interface, weather_interface)

    def collect_data(self, date):
        self.demand_model.collect_data(date)
        self.weather_model.collect_data(date)

        input = []

        market_result = self.market.get_auction_results(date)
        input.append(market_result['volume'])
        input.append(self.weather.get_wind(date))
        input.append(self.weather.get_diffuse_radiation(date))
        input.append(self.weather.get_direct_radiation(date))
        input.append(self.weather.get_temperature(date))
        price_last_week = pd.DataFrame(data=dict(price7d=self.price_register[-1]),
                                       index=pd.date_range(start=date, freq='h', periods=24))
        input.append(price_last_week)
        input.append(create_dummies(date))
        input = pd.concat(input, axis=1).to_numpy()

        output = market_result['price'].to_numpy()

        self.input.append(input)
        self.output.append(output)

        self.price_register.append(output)

    def fit_model(self):
        self.scale.fit(np.asarray(self.input).reshape(-1, 55))
        x_std = self.scale.transform(np.asarray(self.input).reshape(-1, 55))
        self.model.fit(x_std, np.asarray(self.output).reshape(-1, ))
        self.fitted = True
        self.score = self.model.score(x_std, np.asarray(self.output).reshape(-1, ))


    def forecast(self, date):
        if not self.fitted:
            power_price = default_power_price
        else:
            input = []
            input.append(self.demand_model.forecast(date))
            weather = self.weather_model.forecast(date)
            input.append(weather['wind_speed'])
            input.append(weather['direction'])
            input.append(weather['dni'])
            input.append(weather['dhi'])
            input.append(weather['temp_air'])
            price_last_week = pd.DataFrame(data=dict(price7d=self.price_register[-1]),
                                           index= pd.date_range(start=date, freq='h', periods=24))
            input.append(price_last_week)
            input.append(create_dummies(date))
            x = pd.concat(input, axis=1).to_numpy()
            self.scale.partial_fit(x.reshape(-1, 55))
            x_std = self.scale.transform(x.reshape(-1, 55))
            power_price = self.model.predict(x_std).reshape((24,))

        prices = dict(
            power=power_price,
            co=np.ones_like(power_price) * default_emission[date.month - 1] * np.random.uniform(0.95, 1.05, 24),
            gas=np.ones_like(power_price) * default_gas[date.month - 1] * np.random.uniform(0.95, 1.05, 24),
            coal=np.ones_like(power_price) * default_coal * np.random.uniform(0.95, 1.05),
            lignite = np.ones_like(power_price) * default_lignite * np.random.uniform(0.95, 1.05),
            nuclear=np.ones_like(power_price) * np.random.uniform(0.95, 1.05)
        )

        df = pd.DataFrame(index=pd.date_range(start=date, periods=24, freq='h'), data=prices)
        df.index.name = 'time'

        return df
