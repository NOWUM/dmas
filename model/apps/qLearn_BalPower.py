import numpy as np
from numpy import asarray as array
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from keras import models, layers, optimizers
from pyswarm import pso

class learnBalancingPower:

    def __init__(self, initT = 10):

        # -- Optimization Parameter for function fit
        opt = optimizers.Adam(learning_rate=0.001, beta_1=0.9, beta_2=0.999, amsgrad=False)

        # ----- Decision between Day Ahead and Balancing -----
        self.scaler = StandardScaler()                  # -- Scaler to norm the input parameter
        self.function = models.Sequential()             # -- Model to fit the profit function
        # -- Build model structure
        self.function.add(layers.Dense(320, activation='sigmoid', input_shape=(153,)))
        self.function.add(layers.Dense(160, activation='relu'))
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
        # -- split in test & train
        X_train, X_test, y_train, y_test = train_test_split(Xstd, y, test_size=0.3)

        self.function.fit(X_train, y_train, epochs=200, batch_size=5, validation_data=(X_test, y_test))

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

        return pso(self.opt, self.lb, self.ub, omega=0.1, swarmsize=100, phip=0.75, phig=0.3,
                   minfunc=1000, minstep=10, maxiter=100, args=(weather, states, date, prices))