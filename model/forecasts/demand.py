from forecasts.basic_forecast import BasicForecast
import numpy as np
import pandas as pd
from collections import deque

with open(r'./forecasts/data/default_demand.pkl', 'rb') as file:
    default_demand = np.load(file).reshape((24,))


class DemandForecast(BasicForecast):

    def __init__(self):
        super().__init__()
        self.model = {i: [] for i in range(7)}

    def collect_data(self, date):
        t = pd.date_range(start=date, periods=24, freq='h')
        demand = self.simulation_database.get_demand(date)
        for i in range(24):
            self.input.append(t[i])
            self.output.append(demand[i])

    def fit_model(self):
        df = pd.DataFrame.from_dict({self.input[i]: self.output[i] for i in range(len(self.input))}, orient='index')
        for i in range(7):
            day = df.loc[df.index.dayofweek == i,:]
            self.model[i] = day.groupby(day.index.hour).mean().to_numpy()

    def forecast(self, date):
        if self.fitted:
            demand = self.model[int(pd.to_datetime(date)).dayofweek].reshape((-1,))
        else:
            demand = default_demand

        return demand

