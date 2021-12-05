import numpy as np
import pandas as pd
from collections import deque

with open(r'./forecasts/data/default_demand.pkl', 'rb') as file:
    default_demand = np.load(file).reshape((24,))

class DemandForecast:

    def __init__(self):

        self.fitted = False
        self.x = deque(maxlen=1000)
        self.y = deque(maxlen=1000)
        self.model = {i: [] for i in range(7)}
        self.counter = 0

    def collect_data(self, date, demand, *args, **kwargs):
        t = pd.date_range(start=date, periods=24, freq='h')
        for i in range(24):
            self.x.append(t[i])
            self.y.append(demand[i])

    def fit_function(self):
        data = {self.x[i]: self.y[i] for i in range(len(self.x))}
        df = pd.DataFrame.from_dict(data, orient='index')
        for i in range(7):
            day = df.loc[df.index.dayofweek == i,:]
            self.model[i] = day.groupby(day.index.hour).mean().to_numpy()

    def forecast(self, date, *args, **kwargs):
        if self.fitted:
            demand = self.model[int(pd.to_datetime(date)).dayofweek].reshape((-1,))
        else:
            demand = default_demand

        return demand

