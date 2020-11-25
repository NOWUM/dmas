# third party modules
import numpy as np
import pandas as pd
from apps.misc_Dummies import createSaisonDummy
from sklearn.preprocessing import MinMaxScaler
from sklearn.model_selection import train_test_split
from sklearn.neural_network import MLPRegressor
from warnings import simplefilter
simplefilter(action='ignore', category=FutureWarning)
from collections import deque


class annFrcst:

    def __init__(self, init=30, pre_train=False):

        self.fitted = False         # flag for fitted or not fitted model
        self.collect = init         # days before a retrain is started
        self.counter = 0

        # initialize neural network and corresponding scaler
        self.model = MLPRegressor(hidden_layer_sizes=(15, 15,), activation='identity',
                                  solver='adam', learning_rate_init=0.02, shuffle=False, max_iter=600,
                                  early_stopping=True)
        self.scaler = MinMaxScaler()

        self.deque_x = deque(maxlen=250)
        self.deque_y = deque(maxlen=250)

        self.x = np.asarray([]).reshape((-1,168))
        self.y = np.asarray([]).reshape((-1,24))

        self.default_prices = pd.read_csv(r'./data/Ref_DA_Prices.csv', sep=';', decimal=',', index_col=0)
        self.default_prices.index = pd.to_datetime(self.default_prices.index)

        if pre_train:               # use historical data to fit a model at the beginning

            with open(r'./data/preTrain_Input.array', 'rb') as file:   # Step 0: load data
                self.x = np.load(file)                                 # input (2017-2018)
            with open(r'./data/preTrain_Output.array', 'rb') as file:
                self.y = np.load(file)                                 # output (2017-2018)
            self.scaler.fit(self.x)                                    # Step 1: scale data
            x_std = self.scaler.transform(self.x)                      # Step 2: split data
            X_train, X_test, y_train, y_test = train_test_split(x_std, self.y, test_size=0.2)
            self.model.fit(X_train, y_train)                           # Step 3: fit model
            self.fitted = True                                         # Step 4: set fitted-flag to true

    def collect_data(self, date, dem, weather, prc, prc_1, prc_7):
        # collect input data
        dummies = createSaisonDummy(date, date).reshape((-1,))
        x = np.hstack((dem, weather['wind'], weather['dir'] + weather['dif'], weather['temp'], prc_1, prc_7, dummies))
        y = prc
        self.deque_x.append(x)
        self.deque_y.append(y)

    def fit_function(self):

        self.x = np.asarray(self.deque_x)
        self.y = np.asarray(self.deque_y)
        self.scaler.fit(self.x)                                         # Step 1: scale data
        x_std = self.scaler.transform(self.x)                           # Step 2: split data
        # X_train, X_test, y_train, y_test = train_test_split(x_std, self.y, test_size=0.2)
        self.model.fit(x_std, self.y)                                   # Step 3: fit model
        self.fitted = True                                              # Step 4: set fitted-flag to true

        if self.collect == 30:
            self.collect = np.random.random_integers(low=5, high=10)


    def forecast(self, date, dem, weather, prc_1, prc_7):

        if self.fitted:
            # Schritt 0: Aufbau der Arrays
            dem = dem.reshape((-1, 24))
            wnd = (weather['wind'] * np.random.uniform(0.95, 1.05, 24)).reshape((-1, 24))
            rad = ((weather['dir'] + weather['dif']) * np.random.uniform(0.95, 1.05, 24)).reshape((-1, 24))
            tmp = (weather['temp'] * np.random.uniform(0.95, 1.05, 24)).reshape((-1, 24))
            prc_1 = prc_1.reshape((-1, 24))
            prc_7 = prc_7.reshape((-1, 24))
            dummies = createSaisonDummy(date, date).reshape((-1, 24))
            # Schritt 1: Skalieren der Daten
            x = np.concatenate((dem, rad, wnd, tmp, prc_1, prc_7, dummies), axis=1)
            self.scaler.partial_fit(x)
            x_std = self.scaler.transform(x)
            # Schritt 2: Berechnung des Forecasts
            power_price = self.model.predict(x_std).reshape((24,))
        else:
            mcp = self.default_prices.loc[self.default_prices.index.date == pd.to_datetime(date),'price'].to_numpy()
            power_price = np.asarray(mcp).reshape((-1,))

        co = np.ones_like(power_price) * 23.8 * np.random.uniform(0.95, 1.05, 24)   # -- Emission Price     [€/t]
        gas = np.ones_like(power_price) * 24.8 * np.random.uniform(0.95, 1.05, 24)  # -- Gas Price          [€/MWh]
        lignite = 1.5 * np.random.uniform(0.95, 1.05)                               # -- Lignite Price      [€/MWh]
        coal = 9.9 * np.random.uniform(0.95, 1.05)                                  # -- Hard Coal Price    [€/MWh]
        nuc = 1.0 * np.random.uniform(0.95, 1.05)                                   # -- nuclear Price      [€/MWh]

        return dict(power=power_price, gas=gas, co=co, lignite=lignite, coal=coal, nuc=nuc)
