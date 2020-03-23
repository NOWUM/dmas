import numpy as np
from numpy import asarray as array
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.neural_network import MLPRegressor
from pyswarm import pso
import datetime

class annLearn:

    def __init__(self, initT=20):

        self.scaler = StandardScaler()                  # -- Scaler to norm the input parameter
        self.function = MLPRegressor(hidden_layer_sizes=(300, 300,), activation='tanh', solver='sgd',
                                     learning_rate='adaptive', shuffle=False, early_stopping=True)

        # -- Input Parameter
        self.input = dict(weather=[], prices=[], dates=[], states=[], actions=[], Qs=[])

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
            Xstd = Xstd[:len(y),:]
        elif len(Xstd) < len(y):
            y = y[:len(Xstd),:]
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