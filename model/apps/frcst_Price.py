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

    def __init__(self, init=np.random.random_integers(5, 10 + 1), pre_train=True):

        self.fitted = False         # flag for fitted or not fitted model
        self.collect = init         # days before a retrain is started
        self.counter = 0

        # initialize neural network and corresponding scaler
        self.model = MLPRegressor(hidden_layer_sizes=(15, 15,), activation='identity', early_stopping=True,
                                  solver='adam', learning_rate_init=0.02, shuffle=True, max_iter=500)
        self.scaler = MinMaxScaler()

        self.deque_x = deque(maxlen=1000)
        self.deque_y = deque(maxlen=1000)

        self.x = np.asarray([]).reshape((-1,168))
        self.y = np.asarray([]).reshape((-1,24))

        self.default_power = pd.read_csv(r'./data/history_prices.csv', sep=';', decimal=',', index_col=0)
        self.default_power.index = pd.to_datetime(self.default_power.index, infer_datetime_format=True)
        # month mean values year 2018
        self.default_gas = [18.6, 20.0, 24.2, 19.5, 21.7, 22.0, 22.4, 23.8, 27.8, 26.0, 24.9, 24.1]
        # month mean values year 2018
        self.default_emission = [8.3, 9.5, 11.5, 13.3, 14.8, 15.2, 16.4, 18.9, 21.4, 19.6, 19.2, 22.4]
        # enervis hpfc Best Guess
        self.default_coal = 65.18 / 8.141           # €/ske --> €/MWh
        # agora Deutsche "Braunkohlewirtschaft"
        self.default_lignite = 1.5

        self.score = 0.

        if pre_train:               # use historical data to fit a model at the beginning
            with open(r'./data/preTrain_Input.array', 'rb') as file:    # Step 0: load data
                x = np.load(file)                                       # data (2017-2018)
                for line in range(len(x)):
                    self.deque_x.append(x[line,:])
            with open(r'./data/preTrain_Output.array', 'rb') as file:
                y = np.load(file)                                       # output (2017-2018)
                for line in range(len(y)):
                    self.deque_y.append(y[line,:])

            self.x = np.asarray(self.deque_x)
            self.y = np.asarray(self.deque_y)
            self.scaler.fit(self.x)                                    # Step 1: scale data
            x_std = self.scaler.transform(self.x)                      # Step 2: split data
            self.model.fit(x_std, self.y)                              # Step 3: fit model
            self.fitted = True                                         # Step 4: set fitted-flag to true
            self.score = self.model.score(x_std, self.y)

    def collect_data(self, date, dem, weather, prc, prc_1, prc_7):
        # collect data data
        dummies = createSaisonDummy(date, date).reshape((-1,))
        x = np.hstack((dem, weather['wind'], weather['dir'] + weather['dif'], weather['temp'], prc_1, prc_7, dummies))
        y = prc
        self.deque_x.append(x)
        self.deque_y.append(y)

    def fit_function(self):

        self.x = np.asarray(self.deque_x)
        self.y = np.asarray(self.deque_y)
        self.scaler.fit(self.x)                                         # Step 1: scale data
        x_std = self.scaler.transform(self.x)
        self.model.fit(x_std, self.y)                                   # Step 3: fit model
        self.fitted = True                                              # Step 4: set fitted-flag to true
        self.score = self.model.score(x_std, self.y)


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
            mcp = self.default_power.loc[self.default_power.index.date == pd.to_datetime(date),'price'].to_numpy()
            power_price = np.asarray(mcp).reshape((-1,))

        # Emission Price        [€/t]
        co = np.ones_like(power_price) * self.default_emission[date.month - 1]  * np.random.uniform(0.95, 1.05, 24)
        # Gas Price             [€/MWh]
        gas = np.ones_like(power_price) * self.default_gas[date.month - 1] * np.random.uniform(0.95, 1.05, 24)
        # Hard Coal Price       [€/MWh]
        coal = self.default_coal * np.random.uniform(0.95, 1.05)
        # Lignite Price         [€/MWh]
        lignite = self.default_lignite * np.random.uniform(0.95, 1.05)
        # -- Nuclear Price      [€/MWh]
        nuc = 1.0 * np.random.uniform(0.95, 1.05)

        return dict(power=power_price, gas=gas, co=co, lignite=lignite, coal=coal, nuc=nuc)


if __name__ == "__main__":

    from interfaces.interface_Influx import InfluxInterface
    from apps.frcst_Dem import typFrcst
    from apps.frcst_Weather import weatherForecast
    from matplotlib import pyplot as plt
    import pickle

    valid_data = pd.read_pickle(r'./data/validGeneration.pkl')
    valid_data.index = pd.date_range(start='2018-01-01', end='2019-12-31 23:45:00', freq='15min', tz='UTC')
    demandX = valid_data.loc[:, 'powerDemand']
    demandX = demandX.resample('h').mean()

    my_influx = InfluxInterface(database='MAS2020_40', host='149.201.88.70')
    my_demand = typFrcst()
    my_weather = weatherForecast(influx=my_influx)
    my_frcst = annFrcst(pre_train=True)

    x = []
    z = []
    original_price = pd.read_csv(r'./data/history_prices.csv', index_col=0, decimal=',', sep=';')
    original_price.index = pd.to_datetime(original_price.index, infer_datetime_format=True)

    for d in pd.date_range(start='2018-01-01', end='2018-12-31', freq='d'):
        # demand = my_demand.forecast(d)
        demand = demandX.loc[demandX.index.date == d].to_numpy()
        weather = my_weather.forecast(geo='u302eujrq6vg', date=d, mean=True)
        # price = original_price.loc[original_price.index.date == d, 'price'].to_numpy()
        price_d1 = original_price.loc[original_price.index.date == d - pd.DateOffset(days=1), 'price'].to_numpy()
        price_d7 = original_price.loc[original_price.index.date == d - pd.DateOffset(days=7), 'price'].to_numpy()
        price = my_frcst.forecast(d, demand, weather, price_d1, price_d7)
        x.append(price['power'])
        z.append(original_price.loc[original_price.index.date == d, 'price'].to_numpy())


        # my_frcst.collect_data(d, demand, weather, price, price_d1, price_d7)
    #
    # my_frcst.fit_function()
    # print(my_frcst.model.score(my_frcst.scaler.transform(np.asarray(my_frcst.deque_x)), np.asarray(my_frcst.deque_y)))

        # price_d1 = my_influx.get_prc_da(d - pd.DateOffset(days=1)).reshape((-1, 24))
        # price_d7 = my_influx.get_prc_da(d - pd.DateOffset(days=7 )).reshape((-1, 24))
        # dummies = createSaisonDummy(d, d).reshape((-1, 24))


        # price = my_frcst.forecast(d, demand, weather, price_d1, price_d7)
        # x.append(price['power'])
        # z.append(original_price.loc[original_price.index.date == d, 'price'].to_numpy())

    plt.plot(np.asarray(z).reshape((-1,)))
    plt.plot(np.asarray(x).reshape((-1,)))



    # dem = demand.reshape((-1, 24))
    # wnd = (weather['wind'] * np.random.uniform(0.95, 1.05, 24)).reshape((-1, 24))
    # rad = ((weather['dir'] + weather['dif']) * np.random.uniform(0.95, 1.05, 24)).reshape((-1, 24))
    # tmp = (weather['temp'] * np.random.uniform(0.95, 1.05, 24)).reshape((-1, 24))
    # prc_1 = price_d1.reshape((-1, 24))
    # prc_7 = price_d7.reshape((-1, 24))
    # dummies = createSaisonDummy(d, d).reshape((-1, 24))
    # # Schritt 1: Skalieren der Daten
    # data = np.concatenate((dem, rad, wnd, tmp, prc_1, prc_7, dummies), axis=1)