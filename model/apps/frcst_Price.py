# third party modules
import numpy as np
from apps.misc_Dummies import createSaisonDummy
from sklearn.preprocessing import MinMaxScaler
from sklearn.model_selection import train_test_split
from sklearn.neural_network import MLPRegressor
from sklearn.exceptions import ConvergenceWarning
from warnings import simplefilter
simplefilter(action='ignore', category=FutureWarning)


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

        # intput data for neural network
        self.demMatrix = np.array([]).reshape((-1, 24))                # total demand         [MW]
        self.wndMatrix = np.array([]).reshape((-1, 24))                # mean wind peed       [m/s]
        self.radMatrix = np.array([]).reshape((-1, 24))                # mean total radiation [W/m²]
        self.tmpMatrix = np.array([]).reshape((-1, 24))                # mean temperature     [°C]
        self.prc_1Matrix = np.array([]).reshape((-1, 24))              # DA price yesterday   [€/MWh]
        self.prc_7Matrix = np.array([]).reshape((-1, 24))              # DA price week before [€/MWh]
        self.dummieMatrix = np.array([]).reshape((-1, 24))             # dummies
        # output data for neural network
        self.mcpMatrix = np.array([]).reshape((-1, 24))                # DA price             [€/MWh]

        self.x = np.asarray([]).reshape((-1,168))
        self.y = np.asarray([]).reshape((-1,24))

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
        self.demMatrix = np.concatenate((self.demMatrix, dem.reshape((-1, 24))))
        self.wndMatrix = np.concatenate((self.wndMatrix, weather['wind'].reshape((-1, 24))))
        self.radMatrix = np.concatenate((self.radMatrix, (weather['dir'] + weather['dif']).reshape((-1, 24))))
        self.tmpMatrix = np.concatenate((self.tmpMatrix, weather['temp'].reshape((-1, 24))))
        self.prc_1Matrix = np.concatenate((self.prc_1Matrix, prc_1.reshape((-1, 24))))
        self.prc_7Matrix = np.concatenate((self.prc_7Matrix, prc_7.reshape((-1, 24))))
        self.dummieMatrix = np.concatenate((self.dummieMatrix, createSaisonDummy(date, date).reshape((-1, 24))))
        # collect output data
        self.mcpMatrix = np.concatenate((self.mcpMatrix, prc.reshape((-1, 24))))

    def fit_function(self):
        x = np.concatenate((self.demMatrix, self.radMatrix, self.wndMatrix, self.tmpMatrix,     # Step 0: load (build) data
                            self.prc_1Matrix, self.prc_7Matrix, self.dummieMatrix), axis=1)
        x = np.concatenate((self.x, x), axis=0)                         # input data
        y = np.concatenate((self.y, self.mcpMatrix), axis=0)            # output data
        self.scaler.partial_fit(x)                                      # Step 1: scale data
        x_std = self.scaler.transform(x)                                # Step 2: split data
        X_train, X_test, y_train, y_test = train_test_split(x_std, y, test_size=0.2)
        self.model.fit(X_train, y_train)                                # Step 3: fit model
        self.fitted = True                                              # Step 4: set fitted-flag to true

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
            mcp = [37.70, 35.30, 33.90, 33.01, 33.27, 35.78, 43.17, 50.21, 52.89, 51.18, 48.24, 46.72, 44.23,
                   42.29, 41.60, 43.12, 45.37, 50.95, 55.12, 56.34, 52.70, 48.20, 45.69, 40.25]
            power_price = np.asarray(mcp).reshape((-1,))

        co = np.ones_like(power_price) * 23.8 * np.random.uniform(0.95, 1.05, 24)   # -- Emission Price     [€/t]
        gas = np.ones_like(power_price) * 24.8 * np.random.uniform(0.95, 1.05, 24)  # -- Gas Price          [€/MWh]
        lignite = 1.5 * np.random.uniform(0.95, 1.05)                               # -- Lignite Price      [€/MWh]
        coal = 9.9 * np.random.uniform(0.95, 1.05)                                  # -- Hard Coal Price    [€/MWh]
        nuc = 1.0 * np.random.uniform(0.95, 1.05)                                   # -- nuclear Price      [€/MWh]

        return dict(power=power_price, gas=gas, co=co, lignite=lignite, coal=coal, nuc=nuc)
