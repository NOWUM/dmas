import numpy as np
import pandas as pd
from apps.misc_Dummies import createSaisonDummy
from apps.frcst_DEM import typFrcst as demFrcst
from sklearn.preprocessing import MinMaxScaler
from sklearn.model_selection import train_test_split
from sklearn.neural_network import MLPRegressor
from warnings import simplefilter
simplefilter(action='ignore', category=FutureWarning)


class annFrcst:

    def __init__(self, influx, preTrain=True):

        self.influx = influx
        self.fitted = False

        self.model = MLPRegressor(hidden_layer_sizes=(15, 15,), activation='identity',
                                  solver='adam', learning_rate_init=0.02, shuffle=False, max_iter=400)

        self.scaler = MinMaxScaler()

        self.fitted = False
        self.counter = 0
        self.collect = 5

        self.factor = 0

        # Input Daten des Model
        self.dem = np.array([]).reshape((-1, 24))                # Gesamte Nachfrage [MW]
        self.wnd = np.array([]).reshape((-1, 24))                # mittlere Windgeschwindigkeit [m/s]
        self.rad = np.array([]).reshape((-1, 24))                # mittlere Globalstrahlung [W/m²]
        self.tmp = np.array([]).reshape((-1, 24))                # mittlere Temperatur [°C]
        self.prc_1 = np.array([]).reshape((-1, 24))              # DayAhead Preis gestern [€/MWh]
        self.prc_7 = np.array([]).reshape((-1, 24))              # DayAhead Preis vor einer Woche [€/MWh]
        self.dummies = np.array([]).reshape((-1, 24))            # Dummie-Variablen: Feiertag, Sonntag, Monat, Jahreszeit

        # Outputdaten des Models
        self.mcp = np.array([]).reshape((-1, 24))                # DayAhead Preise (0..24) [€/MWh]

        if preTrain:
            # Schritt 0: Laden der Input und Outputdaten (2017 - 2018)
            with open(r'./data/preTrain_Input.array', 'rb') as file:
                self.x = np.load(file)
            with open(r'./data/preTrain_Output.array', 'rb') as file:
                self.y = np.load(file)
            # Schritt 1: Skalieren der Daten
            self.scaler.fit(self.x)
            x_std = self.scaler.transform(self.x)
            # Schritt 2: Aufteilung in Test- und Trainingsdaten
            X_train, X_test, y_train, y_test = train_test_split(x_std, self.y, test_size=0.2)
            # Schritt 3: Training des Models
            self.model.fit(X_train, y_train)
            # Schritt 4: Ausgabe der Ergebnisse
            print('Score auf den Trainingsdaten: %.2f' % self.model.score(X_train, y_train))
            print('Score auf den Testdaten: %.2f' % self.model.score(X_test, y_test))
            self.fitted = True
            self.factor = 1 - self.model.score(X_test, y_test)


    def collectData(self, date):
        """ Sammelt die Daten, die zum Training benötigt werden """

        # Input Daten
        self.dem = np.concatenate((self.dem, self.influx.getTotalDemand(date).reshape((-1, 24))))
        self.wnd = np.concatenate((self.wnd, self.influx.getWind(date).reshape((-1, 24))))
        self.rad = np.concatenate((self.rad, self.influx.getIrradiation(date).reshape((-1, 24))))
        self.tmp = np.concatenate((self.tmp, self.influx.getTemperature(date).reshape((-1, 24))))
        self.prc_1 = np.concatenate((self.prc_1, self.influx.getDayAheadPrice(date - pd.DateOffset(days=1)).reshape((-1, 24))))
        self.prc_7 = np.concatenate((self.prc_7, self.influx.getDayAheadPrice(date - pd.DateOffset(days=7)).reshape((-1, 24))))
        self.dummies = np.concatenate((self.dummies, createSaisonDummy(date, date).reshape((-1, 24))))
        # Outputdaten
        self.mcp = np.concatenate((self.mcp, self.influx.getDayAheadPrice(date).reshape((-1, 24))))

    def fitFunction(self):
        """ Trainiert das Model mit den gesammelten Daten """

        # Schritt 0: Aufbau der Arrays
        x = np.concatenate((self.dem, self.rad, self.wnd, self.tmp, self.prc_1, self.prc_7, self.dummies), axis=1)
        x = np.concatenate((self.x, x), axis=0)
        y = np.concatenate((self.y, self.mcp), axis=0)
        # Schritt 1: Skalieren der Daten
        x_std = self.scaler.fit_transform(x)
        # Schritt 2: Aufteilung in Test- und Trainingsdaten
        X_train, X_test, y_train, y_test = train_test_split(x_std, y, test_size=0.2)
        # Schritt 3: Training des Models
        self.model.fit(X_train, y_train)
        # Schritt 4: Ausgabe der Ergebnisse
        print('Score auf den Trainingsdaten: %.2f' % self.model.score(X_train, y_train))
        print('Score auf den Testdaten: %.2f' % self.model.score(X_test, y_test))
        self.fitted = True
        self.factor = 1 - self.model.score(X_test, y_test)

    def forecast(self, date, demand=np.zeros(24)):

        if self.fitted:
            # Schritt 0: Aufbau der Arrays
            dem = demand.reshape((-1, 24))
            wnd = self.influx.getWind(date).reshape((-1, 24))
            rad = self.influx.getIrradiation(date).reshape((-1, 24))
            tmp = self.influx.getTemperature(date).reshape((-1, 24))
            prc_1 = self.influx.getDayAheadPrice(date - pd.DateOffset(days=1)).reshape((-1, 24))
            prc_7 = self.influx.getDayAheadPrice(date - pd.DateOffset(days=7)).reshape((-1, 24))
            dummies = createSaisonDummy(date, date).reshape((-1, 24))
            # Schritt 1: Skalieren der Daten
            x = np.concatenate((dem, rad, wnd, tmp, prc_1, prc_7, dummies), axis=1)
            x_std = self.scaler.fit_transform(x)
            # Schritt 2: Berechnung des Forecasts
            power_price = self.model.predict(x_std).reshape((24,))
        else:
            power_price = 25*np.ones(24)

        co = np.ones_like(power_price) * 20                                                 # -- Emission Price     [€/MWh]
        gas = np.ones_like(power_price) * 3                                                 # -- Gas Price          [€/MWh]
        lignite = 1.5                                                                       # -- Lignite Price      [€/MWh]
        coal = 2                                                                            # -- Hard Coal Price    [€/MWh]
        nuc = 1                                                                             # -- nuclear Price      [€/MWh]

        return dict(power=power_price, gas=gas, co=co, lignite=lignite, coal=coal, nuc=nuc)

class typFrcst:

    def __init__(self, influx, preTrain=False):

        self.influx = influx
        self.fitted = False
        self.mcp = []
        self.index = []

        self.typDays = []

        self.collect = 10
        self.counter = 0

    def collectData(self, date):
        self.index.append(pd.date_range(start=date, periods=24, freq='h'))
        self.mcp.append(self.influx.getDayAheadPrice(date))

    def fitFunction(self):

        timeIndex = pd.DatetimeIndex.append(self.index[0],self.index[1])

        for i in np.arange(2,len(self.index)):
            timeIndex = timeIndex.append(self.index[i])
        df = pd.DataFrame(np.asarray(self.mcp).reshape((-1,1)), timeIndex, ['demand'])

        typDays = []
        for i in range(7):
            day = df.loc[df.index.dayofweek == i,:]
            typDays.append(day.groupby(day.index.hour).mean())

        self.fitted = True
        self.typDays = typDays

    def forecast(self, date, demand=np.zeros(24)):

        if self.fitted:
            date = pd.to_datetime(date).dayofweek
            power_price = self.typDays[date].to_numpy().reshape((-1,))
        else:
            power_price = 25*np.ones(24)

        co = np.ones_like(power_price) * 20                                                 # -- Emission Price     [€/MWh]
        gas = np.ones_like(power_price) * 3                                                 # -- Gas Price          [€/MWh]
        lignite = 1.5                                                                       # -- Lignite Price      [€/MWh]
        coal = 2                                                                            # -- Hard Coal Price    [€/MWh]
        nuc = 1                                                                             # -- nuclear Price      [€/MWh]

        return dict(power=power_price, gas=gas, co=co, lignite=lignite, coal=coal, nuc=nuc)