import numpy as np
from sklearn.cluster import KMeans


class qLeran:

    def __init__(self, init=6):

        self.states = KMeans(n_clusters=25, n_init=7, max_iter=200, random_state=np.random.randint(low=1, high=100))

        # input for cluster analyse and best action optimization
        self.demMatrix = np.array([]).reshape((-1, 1))              # matrix to save demands during simulation
        self.dem = np.array([]).reshape((-1, 1))                    # array for current demand
        self.wndMatrix = np.array([]).reshape((-1, 1))              # matrix to save  wind speeds during simulation
        self.wnd = np.array([]).reshape((-1, 1))                    # array for current wind speed
        self.radMatrix = np.array([]).reshape((-1, 1))              # matrix to save radiation during simulation
        self.rad = np.array([]).reshape((-1, 1))                    # array for current radiation
        self.tmpMatrix = np.array([]).reshape((-1, 1))              # matrix to save temperature during simulation
        self.tmp = np.array([]).reshape((-1, 1))                    # array for current temperature
        self.prcMatrix = np.array([]).reshape((-1, 1))              # matrix to save day ahead price during simulation
        self.prc = np.array([]).reshape((-1, 1))                    # array for current day ahead price

        # current state to find the best action
        self.sts = np.array([]).reshape((-1, 1))
        # reward values from day ahead market
        self.qus = np.random.uniform(0, 1, (25, 7))

        self.fitted = False
        self.counter = 0
        self.collect = init

    def collect_data(self, dem, prc, weather):
        # collect demand and set actual values
        # demand
        self.dem = dem[:24].reshape((-1, 1))
        self.demMatrix = np.concatenate((self.demMatrix, self.dem))
        # wind
        self.wnd = weather['wind'][:24].reshape((-1, 1))
        self.wndMatrix = np.concatenate((self.wndMatrix, self.wnd))
        # radiation
        self.rad = (weather['dif'] + weather['dir'])[:24].reshape((-1, 1))
        self.radMatrix = np.concatenate((self.radMatrix, self.rad))
        # temperature
        self.tmp = weather['temp'][:24].reshape((-1, 1))
        self.tmpMatrix = np.concatenate((self.tmpMatrix, self.tmp))
        # day ahead price
        self.prc = prc[:24].reshape((-1, 1))
        self.prcMatrix = np.concatenate((self.prcMatrix, self.prc))

    def fit(self):
        x = np.concatenate((self.wndMatrix, self.radMatrix, self.tmpMatrix, self.demMatrix, self.prcMatrix), axis=0)
        x = x.reshape((5, -1))
        x = x[:, -1 * min(2000, np.size(x, 1)):].T  # -- last 2000 samples
        self.states.fit(x)
        # print('score Kmeans [optimal --> 0]: %.2f' % (self.states.score(x)/len(x)))
        self.fitted = True

    def get_actions(self):
        x = np.concatenate((self.wnd, self.rad, self.tmp, self.dem/1000, self.prc), axis=0)
        x = x.reshape((5, -1))
        states = self.states.predict(x.T)
        self.sts = np.asarray(states, dtype=int).reshape((-1, 1))
        actions = [np.argmax(self.qus[state, :]) for state in states]
        return np.array(actions).reshape((-1,))


if __name__ == "__main__":
    pass
