from apps.misc_Dummies import createSaisonDummy
import numpy as np
from numpy import asarray as array
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from keras import models, layers, optimizers
from pyswarm import pso


class annFrcstDemand:

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

class annFrcstPrice:

    def __init__(self, influx):

        self.influx = influx

        opt = optimizers.Adam(learning_rate=0.001, beta_1=0.9, beta_2=0.999, amsgrad=False)

        self.scaler = StandardScaler()
        self.model = models.Sequential()
        self.model.add(layers.Dense(120, activation='sigmoid', input_shape=(52,)))
        self.model.add(layers.Dense(120, activation='relu'))
        self.model.add(layers.Dense(1, activation='linear'))
        self.model.compile(loss='mean_absolute_percentage_error', optimizer=opt, metrics=['mean_squared_error'])

        self.input = dict(GHI=[], Ws=[], TAmb=[], demand=[], dummies=[])
        self.inputArchiv = dict(GHI=[], Ws=[], TAmb=[], demand=[], dummies=[])
        self.mcp = []
        self.mcpArchiv = []

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


        self.input['demand'].append(np.asarray(demand).reshape((-1,1)))

        query = 'SELECT sum("price") FROM "DayAhead" WHERE time >= \'%s\' and time < \'%s\' GROUP BY time(1h) fill(null)' % (start, end)
        result = self.influx.query(query)
        mcp = [np.round(point['sum'],2) for point in result.get_points()]

        self.mcp.append(np.asarray(mcp).reshape((-1,1)))

    def fitFunction(self):

        demand = np.asarray(self.input['demand']).reshape((-1, 1))
        TAmb = np.asarray(self.input['TAmb']).reshape((-1, 1))
        Ws = np.asarray(self.input['Ws']).reshape((-1, 1))
        Ghi = np.asarray(self.input['GHI']).reshape((-1, 1))

        X = np.concatenate((demand, TAmb, Ws, Ghi), axis=1)
        if not self.fitted:
            self.scaler.fit(X)
            self.fitted = True

        self.scaler.partial_fit(X)
        Xstd = self.scaler.transform(X)
        dummies = np.asarray(self.input['dummies']).reshape((-1, 48))
        Xstd = np.concatenate((Xstd, dummies), axis=1)

        y = np.asarray(self.mcp).reshape((-1, 1))

        X_train, X_test, y_train, y_test = train_test_split(Xstd, y, test_size=0.35)

        self.model.fit(X_train, y_train, epochs=200, batch_size=15, validation_data=(X_test, y_test))

    def forecast(self, date, demand):

        if self.fitted:
            # -- create start and end date for query and dummies
            start = date.isoformat() + 'Z'
            end = (date + pd.DateOffset(days=1)).isoformat() + 'Z'

            # -- create dummies for whole year and select the corresponding day
            dummies = createSaisonDummy(pd.to_datetime('%i-01-01' % date.year),
                                        pd.to_datetime('%i-01-01' % (date + pd.DateOffset(years=1)).year), hour=True)
            index = pd.date_range(start=date, periods=24, freq='h')
            dummies = dummies.loc[index, :].to_numpy()

            # -- query weather data (mean complete germany)
            lst = []
            for data in ['TAmb', 'GHI', 'Ws']:
                query = 'select mean(%s) from "weather" where time >= \'%s\' and time < \'%s\' GROUP BY time(1h) fill(null)' % (
                    data, start, end)
                result = self.influx.query(query)
                lst.append([point['mean'] for point in result.get_points()])

            demand = np.asarray(demand).reshape((-1,1))
            TAmb = np.asarray(lst[0]).reshape((-1, 1))
            Ghi = np.asarray(lst[1]).reshape((-1, 1))
            Ws = np.asarray(lst[2]).reshape((-1, 1))

            X = np.concatenate((demand.reshape(-1, 1), TAmb.reshape(-1, 1), Ws.reshape(-1, 1), Ghi.reshape(-1, 1)), axis=1)
            Xstd = self.scaler.transform(X)
            Xstd = np.concatenate((Xstd, dummies), axis=1)
            price = self.model.predict(Xstd)

            power_price = price.reshape((-1,))                                              # -- Power Price        [€/MWh]

        else:
            power_price = 25*np.ones(24)

        co = np.ones_like(power_price) * 20                                                 # -- Emission Price     [€/MWh]
        gas = np.ones_like(power_price) * 3                                                 # -- Gas Price          [€/MWh]
        lignite = 1.5                                                                       # -- Lignite Price      [€/MWh]
        coal = 2                                                                            # -- Hard Coal Price    [€/MWh]
        nuc = 1                                                                             # -- nuclear Price      [€/MWh]

        return dict(power=power_price, gas=gas, co=co, lignite=lignite, coal=coal, nuc=nuc)

class learnBalancingPower:

    def __init__(self, initT = 10):

        # -- Optimization Parameter for function fit
        opt = optimizers.Adam(learning_rate=0.001, beta_1=0.9, beta_2=0.999, amsgrad=False)

        # ----- Decision between Day Ahead and Balancing -----
        self.scaler = StandardScaler()                  # -- Scaler to norm the input parameter
        self.function = models.Sequential()             # -- Model to fit the profit function
        # -- Build model structure
        self.function.add(layers.Dense(153, activation='sigmoid', input_shape=(153,)))
        self.function.add(layers.Dense(160, activation='relu'))
        self.function.add(layers.Dense(1, activation='linear'))
        self.function.compile(loss='mean_absolute_percentage_error', optimizer=opt, metrics=['mean_squared_error'])

        # -- Input Parameter
        self.input = dict(weather=[], prices=[], dates=[], states=[], actions=[], Qs=[])
        self.archiv = dict(weather=[], prices=[], dates=[], states=[], actions=[], Qs=[])

        # -- Lower and Upper Bound for Action-Optimization
        self.lb = np.zeros(30)
        self.ub = np.ones(30).reshape((6,-1))
        self.ub[:, 1:] *= 500
        self.ub = self.ub.reshape((-1,))

        self.collect = initT
        self.counter = 0
        self.randomPoint = False

    # -- array for function fit
    def buildArray(self, values):
        num = len(self.input['weather'])
        sample = range(num)
        if isinstance(values[0], pd.datetime):
            return array([array(values[i].dayofweek).reshape((1, -1)) for i in sample]).reshape((num, -1))
        else:
            return array([array(values[i]).reshape((1, -1)) for i in sample]).reshape((num, -1))

    # -- fit day ahead balancing function
    def fit(self):
        # -- build Input Matrix
        X = np.concatenate(tuple([self.buildArray(values=value) for key, value in self.input.items()
                                  if key != 'Qs']), axis=1)
        # -- build Output Matrix
        y = array(self.input['Qs']).reshape((-1, 1))
        # -- scale data
        self.scaler.partial_fit(X)
        Xstd = self.scaler.transform(X)
        if len(y) < len(Xstd):
            Xstd = Xstd[len(y),:]
        elif len(Xstd) < len(y):
            y = y[:len(Xstd),:]
        # -- split in test & train
        X_train, X_test, y_train, y_test = train_test_split(Xstd, y, test_size=0.3)

        self.function.fit(X_train, y_train, epochs=75, batch_size=25, validation_data=(X_test, y_test))

        print('val_loss: %s' %self.function.history.history['val_loss'][-1])
        print('loss: %s' %self.function.history.history['loss'][-1])

        if len(self.input['weather']) >= 350:
            for _ in range(self.collect):
                for key in self.input.keys():
                    self.archiv[key].append(self.input[key].pop(0))

        if not self.randomPoint:
            self.randomPoint = True

    # -- get optimal parameter setting for day ahead balancing decison
    def opt(self, x, *args):

        day = (pd.to_datetime(str(args[2]))).dayofweek      # -- date
        weather = array(args[0]).reshape((1, -1))           # -- weather
        states = array(args[1]).reshape((1, -1))            # -- state
        date = array(day).reshape((1, -1))                  # -- day as int
        prices = array(args[3]).reshape((1, -1))            # -- prices
        actions = array(x).reshape((1, -1))                 # -- actions
        # -- care of argument order
        X = np.concatenate((weather, prices, date, states, actions), axis=1)
        self.scaler.partial_fit(X)

        return -1*float(self.function.predict(self.scaler.transform(X)))

    def getAction(self, weather, states, date, prices):

        return pso(self.opt, self.lb, self.ub, omega=0.1, swarmsize=10, phip=0.75, phig=0.3,
                   minfunc=1000, minstep=10, maxiter=50, args=(weather, states, date, prices))

class learnDayAheadMarginal:

    def __init__(self, initT = 10):

        # -- Optimization Parameter for function fit
        opt = optimizers.Adam(learning_rate=0.001, beta_1=0.9, beta_2=0.999, amsgrad=False)

        # ----- Decision between Day Ahead and Balancing -----
        self.scaler = StandardScaler()          # -- Scaler to norm the input parameter
        self.function = models.Sequential()     # -- Model to fit the profit function
        # -- Build model structure
        self.function.add(layers.Dense(220, activation='sigmoid', input_shape=(217,)))
        self.function.add(layers.Dense(220, activation='relu'))
        self.function.add(layers.Dense(1, activation='linear'))
        self.function.compile(loss='mean_absolute_percentage_error', optimizer=opt, metrics=['mean_squared_error'])

        # -- Input Parameter
        self.input = dict(weather=[], prices=[], dates=[], states=[], actions=[], demand=[], Qs=[])
        self.archiv = dict(weather=[], prices=[], dates=[], states=[], actions=[], demand=[], Qs=[])

        # -- Lower and Upper Bound for Action-Optimization
        self.lb = -20 * np.ones(24)
        self.ub = 20 * np.ones(24)

        self.collect = initT
        self.counter = 0
        self.randomPoint = False

    # -- array for function fit
    def buildArray(self, values):
        num = len(self.input['weather'])
        sample = range(num)
        if isinstance(values[0], pd.datetime):
            return array([array(values[i].dayofweek).reshape((1, -1)) for i in sample]).reshape((num, -1))
        else:
            return array([array(values[i]).reshape((1, -1)) for i in sample]).reshape((num, -1))

    # -- fit day ahead balancing function
    def fit(self):
        # -- build Input Matrix
        X = np.concatenate(tuple([self.buildArray(values=value) for key, value in self.input.items()
                                  if key != 'Qs']), axis=1)
        # -- build Output Matrix
        y = array(self.input['Qs']).reshape((-1, 1))
        # -- scale data
        self.scaler.partial_fit(X)
        Xstd = self.scaler.transform(X)
        if len(y) < len(Xstd):
            Xstd = Xstd[len(y),:]
        elif len(Xstd) < len(y):
            y = y[:len(Xstd),:]
        # -- split in test & train
        X_train, X_test, y_train, y_test = train_test_split(Xstd, y, test_size=0.3)

        self.function.fit(X_train, y_train, epochs=75, batch_size=25, validation_data=(X_test, y_test))

        print('val_loss: %s' %self.function.history.history['val_loss'][-1])
        print('loss: %s' %self.function.history.history['loss'][-1])

        if len(self.input['weather']) >= 350:
            for _ in range(self.collect):
                for key in self.input.keys():
                    self.archiv[key].append(self.input[key].pop(0))

        if not self.randomPoint:
            self.randomPoint = True

    # -- get optimal parameter setting for day ahead balancing decison
    def opt(self, x, *args):

        day = (pd.to_datetime(str(args[2]))).dayofweek      # -- date
        weather = array(args[0]).reshape((1, -1))           # -- weather
        states = array(args[1]).reshape((1, -1))            # -- state
        date = array(day).reshape((1, -1))                  # -- day as int
        prices = array(args[3]).reshape((1, -1))            # -- prices
        demand = array(args[4]).reshape((1,-1))             # -- demand
        actions = array(x).reshape((1, -1))                 # -- actions
        # -- care of argument order
        X = np.concatenate((weather, prices, date, states, actions, demand), axis=1)

        self.scaler.partial_fit(X)

        return -1*float(self.function.predict(self.scaler.transform(X)))

    def getAction(self, weather, states, date, prices, demand):

        return pso(self.opt, self.lb, self.ub, omega=0.1, swarmsize=10, phip=0.75, phig=0.3,
                   minfunc=1000, minstep=10, maxiter=50, args=(weather, states, date, prices, demand))

    class annLearn:

        def __init__(self, initT=10):

            self.scaler = StandardScaler()  # -- Scaler to norm the input parameter
            self.function = MLPRegressor(hidden_layer_sizes=(300, 300,), activation='tanh', solver='sgd',
                                         learning_rate='adaptive', shuffle=False, early_stopping=True)

            # -- Input Parameter
            self.input = dict(weather=[], prices=[], dates=[], states=[], actions=[], demand=[], Qs=[])

            # -- Lower and Upper Bound for Action-Optimization
            self.lb = -20 * np.ones(24)
            self.ub = 20 * np.ones(24)

            self.collect = initT
            self.counter = 0
            self.randomPoint = False

        # -- array for function fit
        def buildArray(self, values):
            num = len(self.input['weather'])
            sample = range(num)
            if isinstance(values[0], datetime.datetime):
                return array([array(values[i].dayofweek).reshape((1, -1)) for i in sample]).reshape((num, -1))
            else:
                return array([array(values[i]).reshape((1, -1)) for i in sample]).reshape((num, -1))

        # -- fit day ahead balancing function
        def fit(self):
            # -- build Input Matrix
            X = np.concatenate(tuple([self.buildArray(values=value) for key, value in self.input.items()
                                      if key != 'Qs']), axis=1)
            # -- build Output Matrix
            y = array(self.input['Qs']).reshape((-1, 1))
            # -- scale data
            self.scaler.partial_fit(X)
            Xstd = self.scaler.transform(X)
            if len(y) < len(Xstd):
                Xstd = Xstd[:len(y), :]
            elif len(Xstd) < len(y):
                y = y[:len(Xstd), :]
            # -- split in test & train
            X_train, X_test, y_train, y_test = train_test_split(Xstd, y, test_size=0.1)

            self.function.fit(X_train, y_train.reshape((-1,)))
            scoreTrain = self.function.score(X_train, y_train)
            scoreTest = self.function.score(X_test, y_test)
            print('Score Train: %s' % scoreTrain)
            print('Score Test: %s' % scoreTest)

            if scoreTrain >= 0.2:
                self.randomPoint = False

        # -- get optimal parameter setting for day ahead balancing decison
        def opt(self, x, *args):

            day = (pd.to_datetime(str(args[2]))).dayofweek  # -- date
            weather = array(args[0]).reshape((1, -1))  # -- weather
            states = array(args[1]).reshape((1, -1))  # -- state
            date = array(day).reshape((1, -1))  # -- day as int
            prices = array(args[3]).reshape((1, -1))  # -- prices
            demand = array(args[4]).reshape((1, -1))  # -- demand
            actions = array(x).reshape((1, -1))  # -- actions
            # -- care of argument order
            X = np.concatenate((weather, prices, date, states, actions, demand), axis=1)

            self.scaler.partial_fit(X)

            return -1 * float(self.function.predict(self.scaler.transform(X)))

        def getAction(self, weather, states, date, prices, demand):

            return pso(self.opt, self.lb, self.ub, omega=0.1, swarmsize=10, phip=0.75, phig=0.3,
                       minfunc=1000, minstep=10, maxiter=50, args=(weather, states, date, prices, demand))

    def optimize_balancing(self):
        # -- set parameter for optimization
        self.portfolio.setPara(self.date, self.weatherForecast(), self.priceForecast(), self.demandForecast())

        # -- save the status for learning
        self.intelligence['Balancing'].input['weather'].append([x[1] for x in agent.portfolio.weather.items()])
        self.intelligence['Balancing'].input['dates'].append(self.portfolio.date)
        self.intelligence['Balancing'].input['prices'].append(self.portfolio.prices['power'])
        self.intelligence['Balancing'].input['states'].append(self.getStates()[0])

        # -- get best split between dayAhead and balancing market
        if self.intelligence['Balancing'].randomPoint:
            xopt, _ = self.intelligence['Balancing'].getAction([x[1] for x in agent.portfolio.weather.items()], self.getStates()[0],
                                                               self.portfolio.date, self.priceForecast()['power'])
            xopt = np.asarray(xopt).reshape((6,-1))

        # -- build up orderbook
        actions = []
        orders = dict(uuid=self.name, date=str(self.date))
        for i in range(6):
            # -- amount of power
            a = np.random.uniform(low=0, high=1)
            # -- power prices
            powerPricePos = np.random.uniform(low=100, high=500)
            powerPriceNeg = np.random.uniform(low=100, high=500)
            # -- energy prices
            energyPricePos = np.random.uniform(low=0, high=50)
            energyPriceNeg = np.random.uniform(low=0, high=50)
            if self.intelligence['Balancing'].randomPoint:
                a = xopt[i,0]
                powerPricePos = xopt[i,1]
                powerPriceNeg = xopt[i,2]
                energyPricePos = xopt[i,3]
                energyPriceNeg = xopt[i,4]
            # -- append actions
            actions.append([a, powerPricePos, energyPricePos, powerPriceNeg, energyPriceNeg])

            # -- build orders
            orders.update({str(i) + '_pos': (np.round(self.getStates()[1][0][i] * a, 0), powerPricePos, energyPricePos)})
            orders.update({str(i) + '_neg': (np.round(self.getStates()[1][1][i] * a, 0), powerPriceNeg, energyPriceNeg)})

        # -- save actions for learning
        self.intelligence['Balancing'].input['actions'].append(actions)
        # -- send orderbook to market plattform
        self.restCon.sendBalancing(orders)

        def getStates(self):

            self.portfolio.setPara(weather=self.weather_forecast(), date=self.date, prices={})
            self.portfolio.buildModel()
            power = np.asarray(self.portfolio.optimize(), np.float) * 0.95

            states = [[min(power[i:i + 4]) / 2 for i in range(0, 24, 4)],
                      [min(power[i:i + 4]) / 2 for i in range(0, 24, 4)]]

            return [np.mean(power), np.min(power)], states

    class annLearn:

        def __init__(self, initT=20):

            self.scaler = StandardScaler()  # -- Scaler to norm the input parameter
            self.function = MLPRegressor(hidden_layer_sizes=(300, 300,), activation='tanh', solver='sgd',
                                         learning_rate='adaptive', shuffle=False, early_stopping=True)

            # -- Input Parameter
            self.input = dict(weather=[], prices=[], dates=[], states=[], actions=[], Qs=[])

            # -- Lower and Upper Bound for Action-Optimization
            self.lb = np.zeros(30)
            self.ub = np.ones(30).reshape((6, -1))
            self.ub[:, 1] *= 500  # -- Power Price Pos
            self.ub[:, 2] *= 500  # -- Power Price Neg
            self.ub[:, 3] *= 50  # -- Energy Price Pos
            self.ub[:, 4] *= 50  # -- Energy Price Neg
            self.ub = self.ub.reshape((-1,))

            self.collect = initT
            self.counter = 0
            self.randomPoint = False

        # -- array for function fit
        def buildArray(self, values):
            num = len(self.input['weather'])
            sample = range(num)
            if isinstance(values[0], datetime.datetime):
                return array([array(values[i].dayofweek).reshape((1, -1)) for i in sample]).reshape((num, -1))
            else:
                return array([array(values[i]).reshape((1, -1)) for i in sample]).reshape((num, -1))

        # -- fit day ahead balancing function
        def fit(self):
            # -- build Input Matrix
            X = np.concatenate(tuple([self.buildArray(values=value) for key, value in self.input.items()
                                      if key != 'Qs']), axis=1)
            # -- build Output Matrix
            y = array(self.input['Qs']).reshape((-1, 1))
            # -- scale data
            self.scaler.partial_fit(X)
            Xstd = self.scaler.transform(X)
            if len(y) < len(Xstd):
                Xstd = Xstd[:len(y), :]
            elif len(Xstd) < len(y):
                y = y[:len(Xstd), :]
            # -- split in test & train
            X_train, X_test, y_train, y_test = train_test_split(Xstd, y, test_size=0.1)

            self.function.fit(X_train, y_train.reshape((-1,)))
            scoreTrain = self.function.score(X_train, y_train)
            scoreTest = self.function.score(X_test, y_test)
            print('Score Train: %s' % scoreTrain)
            print('Score Test: %s' % scoreTest)

            if scoreTrain >= 0.2:
                self.randomPoint = False

        # -- get optimal parameter setting for day ahead balancing decison
        def opt(self, x, *args):

            day = (pd.to_datetime(str(args[2]))).dayofweek  # -- date
            weather = array(args[0]).reshape((1, -1))  # -- weather
            states = array(args[1]).reshape((1, -1))  # -- state
            date = array(day).reshape((1, -1))  # -- day as int
            prices = array(args[3]).reshape((1, -1))  # -- prices
            actions = array(x).reshape((1, -1))  # -- actions
            # -- care of argument order
            X = np.concatenate((weather, prices, date, states, actions), axis=1)
            self.scaler.partial_fit(X)

            return -1 * float(self.function.predict(self.scaler.transform(X)))

        def getAction(self, weather, states, date, prices):

            return pso(self.opt, self.lb, self.ub, omega=0.1, swarmsize=10, phip=0.75, phig=0.3,
                       minfunc=1000, minstep=10, maxiter=50, args=(weather, states, date, prices))



        # ----- balancing power -----
        gradP = [x for x in self.m.getVars() if 'gradUp_' in x.VarName]
        self.m.addConstrs(
            float(self.posBalPower[i]) <= quicksum(x for x in gradP if '[%i]' % i in x.VarName) for i in self.t)

        gradM = [x for x in self.m.getVars() if 'gradDown_' in x.VarName]
        self.m.addConstrs(
            float(self.negBalPower[i]) <= quicksum(x for x in gradM if '[%i]' % i in x.VarName) for i in self.t)

        maxPower = np.asarray(self.Cap_PWP - self.posBalPower)
        self.m.addConstrs((power[i] <= maxPower[i] for i in self.t))
        self.m.addConstrs((power[i] >= float(self.negBalPower[i]) for i in self.t))