# third party modules
import numpy as np
from forecasts.dummies import create_dummies
from sklearn.preprocessing import MinMaxScaler
from sklearn.neural_network import MLPRegressor
from collections import deque

with open(r'./forecasts/data/default_price.pkl', 'rb') as file:
    default_power_price = np.load(file).reshape((24,))                  # hourly mean values 2015-2018
with open(r'./forecasts/data/default_gas.pkl', 'rb') as file:
    default_gas = np.load(file).reshape((24,))                          # month mean values year 2018
with open(r'./forecasts/data/default_emission.pkl', 'rb') as file:
    default_emission = np.load(file).reshape((24,))                     # month mean values year 2018

default_coal = 65.18 / 8.141                                            # €/ske --> €/MWh
default_lignite = 1.5                                                   # agora Deutsche "Braunkohlewirtschaft"

class PriceForecast:

    def __init__(self):

        self.fitted = False         # flag for fitted or not fitted model
        self.counter = 0

        # initialize neural network and corresponding scaler
        self.model = MLPRegressor(hidden_layer_sizes=(15, 15,), activation='identity', early_stopping=True,
                                  solver='adam', learning_rate_init=0.02, shuffle=True, max_iter=500)
        self.scale = MinMaxScaler()

        self.x = deque(maxlen=1000)
        self.y = deque(maxlen=1000)

        self.score = 0.

    def collect_data(self, date, demand, wind, radiation_dir, radiation_dif, temperature,
                     price, price_yesterday, price_last_week, *args, **kwargs):

        self.x.append(np.hstack((demand, wind, radiation_dir + radiation_dif, temperature,
                                       price_yesterday, price_last_week, create_dummies(date))))
        self.y.append(price)

    def fit_function(self):

        self.scale.fit(np.asarray(self.x))              # Step 1: scale data
        x_std = self.scale.transform(self.x)
        self.model.fit(x_std, self.y)                   # Step 2: fit model
        self.fitted = True                              # Step 3: set fitted-flag to true
        self.score = self.model.score(x_std, self.y)

    def forecast(self, date, demand, wind, radiation_dir, radiation_dif, temperature,
                 price_yesterday, price_last_week, *args, **kwargs):

        if self.fitted:
            # Schritt 1: Skalieren der Daten
            x = np.concatenate((demand, radiation_dir + radiation_dif, wind, temperature,
                                price_yesterday, price_last_week, create_dummies(date)), axis=1)
            self.scale.partial_fit(x)
            x_std = self.scale.transform(x)
            # Schritt 2: Berechnung des Forecasts
            power_price = self.model.predict(x_std).reshape((24,))
        else:
            power_price = default_power_price

        # Emission Price        [€/t]
        co = np.ones_like(power_price) * default_emission[date.month - 1]  * np.random.uniform(0.95, 1.05, 24)
        # Gas Price             [€/MWh]
        gas = np.ones_like(power_price) * default_gas[date.month - 1] * np.random.uniform(0.95, 1.05, 24)
        # Hard Coal Price       [€/MWh]
        coal = default_coal * np.random.uniform(0.95, 1.05)
        # Lignite Price         [€/MWh]
        lignite = default_lignite * np.random.uniform(0.95, 1.05)
        # -- Nuclear Price      [€/MWh]
        nuc = 1.0 * np.random.uniform(0.95, 1.05)

        return dict(power=power_price, gas=gas, co=co, lignite=lignite, coal=coal, nuc=nuc)
