from forecasts.basic_forecast import BasicForecast
import numpy as np
import pandas as pd
from datetime import timedelta as td
from collections import deque
import logging

log = logging.getLogger('demand_forecast')

default_demand = pd.read_csv('./forecasts/data/hourly_prices.csv')['demand']

class DemandForecast:
    def __init__(self):
        self.model = {i: [] for i in range(7)}
        self.input = deque(maxlen=400)
        self.output = deque(maxlen=400)
        self.fitted = False

    def collect_data(self, date: pd.Timestamp, demand: pd.Series):
        if len(demand) < 24:
            log.warning('market results are not valid, set default demand.')
            demand = default_demand.copy()
            demand.index = pd.date_range(start=date, freq='h', periods=len(demand))
        for i in range(24):
            self.input.append(demand.index[i])
            self.output.append(demand.values[i])

    def fit_model(self):
        df = pd.DataFrame(index=self.input, data={'demand': self.output})
        for i in range(7):
            day = df.loc[[d.dayofweek == i for d in df.index], :]
            self.model[i] = day.groupby(day.index.hour).mean()
        self.fitted = True

    def _forecast(self, date):
        if self.fitted:
            demand = self.model[int(pd.to_datetime(date).dayofweek)]
            demand.index = pd.date_range(start=date, freq='h', periods=len(demand))
            demand.index.name = 'time'
        else:
            demand = pd.DataFrame(index=pd.date_range(start=date, freq='h', periods=len(default_demand)),
                                  data={'demand': list(default_demand)})
            demand.index.name = 'time'

        return demand

    def forecast(self, date: pd.Timestamp, steps: int = 24):
        if steps % 24 > 0:
            print(f'wrong step size: {steps}')
            steps -= steps % 24
            print(f'set step size to {steps}')
        steps = max(steps, 24)
        range_ = pd.date_range(start=date, end=date + td(days=(steps//24) - 1), freq='d')
        demand = pd.concat([self._forecast(date) for date in range_], axis=0)

        return demand

