import numpy as np
import pandas as pd
from apps.misc_Dummies import createSaisonDummy
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from keras import models, layers, optimizers

class demandForecast:

    def __init__(self, influx):

        self.influx = influx

        opt = optimizers.Adam(learning_rate=0.001, beta_1=0.9, beta_2=0.999, amsgrad=False)

        self.scaler = StandardScaler()
        self.model = models.Sequential()
        self.model.add(layers.Dense(120, activation='sigmoid', input_shape=(49,)))
        self.model.add(layers.Dense(120, activation='relu'))
        self.model.add(layers.Dense(1, activation='linear'))
        self.model.compile(loss='mean_absolute_percentage_error', optimizer=opt, metrics=['mean_squared_error'])

        self.input = dict(GHI=[], Ws=[], TAmb=[], dummies=[])
        self.inputArchiv = dict(GHI=[], Ws=[], TAmb=[], dummies=[])
        self.demand = []
        self.demandArchiv = []

        self.fitted = False
        self.collect = 10
        self.counter = 0

    def collectData(self, date):

        # -- create start and end date for query and dummies
        start = date.isoformat() + 'Z'
        end = (date + pd.DateOffset(days=1)).isoformat() + 'Z'

        # -- create dummies for whole year and select the corresponding day
        dummies = createSaisonDummy(pd.to_datetime('%i-01-01' % date.year),
                                    pd.to_datetime('%i-01-01' % (date + pd.DateOffset(years=1)).year),
                                    hour=True)
        index = pd.date_range(start=date, periods=24, freq='h')
        dummies = dummies.loc[index, :].to_numpy()
        self.input['dummies'].append(dummies)

        lst = []

        for data in ['TAmb', 'GHI', 'Ws']:
            query = 'select mean(%s) from "weather" where time >= \'%s\' and time < \'%s\' GROUP BY time(1h) fill(null)' % (
                data, start, end)
            result = self.influx.query(query)
            lst.append([point['mean'] for point in result.get_points()])

        self.input['TAmb'].append(np.asarray(lst[0]).reshape((-1, 1)))
        self.input['GHI'].append(np.asarray(lst[1]).reshape((-1, 1)))
        self.input['Ws'].append(np.asarray(lst[2]).reshape((-1, 1)))
        query = 'SELECT sum("Power") FROM "Areas" WHERE time >= \'%s\' and time < \'%s\'  and "timestamp" = \'optimize_dayAhead\' GROUP BY time(1h) fill(0)' % (start, end)
        result = self.influx.query(query)
        demand = [np.round(point['sum'],2) for point in result.get_points()]

        self.demand.append(np.asarray(demand).reshape((-1,1)))

    def fitFunction(self):
        X = np.asarray(self.input['TAmb']).reshape((-1, 1))

        if not self.fitted:
            self.scaler.fit(X)
            self.fitted = True

        self.scaler.partial_fit(X)
        Xstd = self.scaler.transform(X)
        dummies = np.asarray(self.input['dummies']).reshape((-1,48))
        Xstd = np.concatenate((Xstd, dummies), axis=1)

        y = np.asarray(self.demand).reshape((-1, 1))

        X_train, X_test, y_train, y_test = train_test_split(Xstd, y, test_size=0.35)

        self.model.fit(X_train, y_train, epochs=200, batch_size=15, validation_data=(X_test, y_test))

    def forecast(self, date):

        if self.fitted:
                    # -- create start and end date for query and dummies
            start = date.isoformat() + 'Z'
            end = (date + pd.DateOffset(days=1)).isoformat() + 'Z'

            # -- create dummies for whole year and select the corresponding day
            dummies = createSaisonDummy(pd.to_datetime('%i-01-01' % date.year),
                                        pd.to_datetime('%i-01-01' % (date + pd.DateOffset(years=1)).year),
                                        hour=True)
            index = pd.date_range(start=date, periods=24, freq='h')
            dummies = dummies.loc[index, :].to_numpy()

            # -- query weather data (mean complete germany)
            lst = []
            for data in ['TAmb', 'GHI', 'Ws']:
                query = 'select mean(%s) from "weather" where time >= \'%s\' and time < \'%s\' GROUP BY time(1h) fill(null)' % (
                    data, start, end)
                result = self.influx.query(query)
                lst.append([point['mean'] for point in result.get_points()])

            TAmb = np.asarray(lst[0]).reshape((-1, 1))
            Xstd = np.concatenate((self.scaler.transform(TAmb), dummies), axis=1)
            demand = self.model.predict(Xstd)

        else:
            demand = 10000 * np.zeros(24)

        return demand