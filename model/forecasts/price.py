# third party modules
import numpy as np
from forecasts.dummies import create_dummies
from sklearn.preprocessing import MinMaxScaler
from sklearn.neural_network import MLPRegressor
from collections import deque


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

    def __init__(self):
        super().__init__()

        # initialize neural network and corresponding scaler
        self.model = MLPRegressor(hidden_layer_sizes=(15, 15,), activation='identity', early_stopping=True,
                                  solver='adam', learning_rate_init=0.02, shuffle=True, max_iter=500)
        self.scale = MinMaxScaler()
        self.score = 0.

        self.price_register = deque(maxlen=8)

        self.demand_model = DemandForecast()
        self.weather_model = WeatherForecast()

    def collect_data(self, date):
        self.demand_model.collect_data(date)
        self.weather_model.collect_data(date)

        demand = self.simulation_database.get_demand(date)
        wind = self.weather_database.get_wind(date)
        radiation = self.weather_database.get_direct_radiation(date) + self.weather_database.get_diffuse_radiation(date)
        temperature = self.weather_database.get_temperature(date)
        price = self.simulation_database.get_power_price(date)

        self.input.append(np.hstack((demand, wind, radiation, temperature, self.price_register[-1],
                                     self.price_register[0], create_dummies(date))))
        self.output.append(price)
        self.price_register.append(price)

    def fit_model(self):

        self.scale.fit(np.asarray(self.input))              # Step 1: scale data
        x_std = self.scale.transform(self.output)
        self.model.fit(x_std, self.output)                  # Step 2: fit model
        self.fitted = True                                  # Step 3: set fitted-flag to true
        self.score = self.model.score(x_std, self.output)

    def forecast(self, date):
        if not self.fitted:
            power_price = default_power_price
        else:
            demand = self.demand_model.forecast(date)
            temperature, wind, radiation = self.weather_model.forecast(date)
            x = np.concatenate((demand, radiation, wind, temperature, self.price_register[-1], self.price_register[0],
                                create_dummies(date)), axis=1)
            self.scale.partial_fit(x)
            x_std = self.scale.transform(x)
            power_price = self.model.predict(x_std).reshape((24,))

        # Emission Price        [€/t]
        co = np.ones_like(power_price) * default_emission[date.month - 1] * np.random.uniform(0.95, 1.05, 24)
        # Gas Price             [€/MWh]
        gas = np.ones_like(power_price) * default_gas[date.month - 1] * np.random.uniform(0.95, 1.05, 24)
        # Hard Coal Price       [€/MWh]
        coal = default_coal * np.random.uniform(0.95, 1.05)
        # Lignite Price         [€/MWh]
        lignite = default_lignite * np.random.uniform(0.95, 1.05)
        # -- Nuclear Price      [€/MWh]
        nuc = 1.0 * np.random.uniform(0.95, 1.05)

        return dict(power=power_price, gas=gas, co=co, lignite=lignite, coal=coal, nuc=nuc)
