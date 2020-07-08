import numpy as np
from sklearn.cluster import KMeans


class qLeran:

    def __init__(self, influx, init=6):

        self.influx = influx
        self.states = KMeans(n_clusters=25, n_init=7, max_iter=200, random_state=np.random.randint(low=1, high=100))

        # -- Input for Cluster-Anaylse
        self.dem = np.array([]).reshape((-1, 1))
        self.wnd = np.array([]).reshape((-1, 1))
        self.rad = np.array([]).reshape((-1, 1))
        self.tmp = np.array([]).reshape((-1, 1))
        self.prc = np.array([]).reshape((-1, 1))
        # -- Action Size (70)
        self.act = np.array([]).reshape((-1, 1))
        # -- Reward Values
        self.qus = np.random.uniform(0, 1, (25, 7))

        self.fitted = False
        self.counter = 0
        self.collect = init

    def collectData(self, date, actions):
        self.dem = np.concatenate((self.dem, self.influx.getTotalDemand(date)/1000))
        self.wnd = np.concatenate((self.wnd, self.influx.getWind(date)))
        self.rad = np.concatenate((self.rad, self.influx.getIrradiation(date)))
        self.prc = np.concatenate((self.prc, self.influx.getDayAheadPrice(date)))
        self.tmp = np.concatenate((self.tmp, self.influx.getTemperature(date)))
        self.act = np.concatenate((self.act, actions))

    def fit(self):
        x = np.concatenate((self.wnd, self.rad, self.tmp, self.dem, self.prc), axis=0)
        x = x.reshape((5, -1))
        x = x[:, -1 * min(2000, np.size(x, 1)):].T  # -- last 2000 samples
        self.states.fit(x)
        print('score Kmeans [optimal --> 0]: %.2f' % (self.states.score(x)/len(x)))
        self.fitted = True

    def getAction(self, wnd, rad, tmp, dem, prc):
        x = np.concatenate((wnd, rad, tmp, dem/1000, prc), axis=0)
        x = x.reshape((5, -1))
        states = self.states.predict(x.T)
        actions = [np.argmax(self.qus[state, :]) for state in states]
        return np.array(actions).reshape((-1,))

    def getStates(self, date):
        x = np.concatenate((self.influx.getWind(date), self.influx.getIrradiation(date), self.influx.getTemperature(date),
                            self.influx.getTotalDemand(date)/1000, self.influx.getDayAheadPrice(date)), axis=0)
        x = x.reshape((5,-1))
        states = self.states.predict(x.T)
        return states

if __name__ == "__main__":
    pass