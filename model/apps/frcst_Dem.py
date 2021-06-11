import numpy as np
import pandas as pd

class typFrcst:

    def __init__(self):

        self.fitted = False                     # flag for fitted or not fitted model
        self.collect = 10                       # days before a retrain is started
        self.counter = 0  # day counter

        self.index = []                         # input data for typ days (date)
        self.demand = []                        # output data (demand)
        self.typDays = []                       # "model" of typ days

    def collect_data(self, date, dem, weather, prc, prc_1, prc_7):
        # collect data
        self.index.append(pd.date_range(start=date, periods=24, freq='h'))
        self.demand.append(dem.reshape(-1, 1))

    def fit_function(self):
        time_index = pd.DatetimeIndex.append(self.index[0], self.index[1])
        for i in np.arange(2, len(self.index)):
            time_index = time_index.append(self.index[i])
        df = pd.DataFrame(np.asarray(self.demand).reshape((-1, 1)), time_index, ['demand'])

        typ_days = []
        for i in range(7):
            day = df.loc[df.index.dayofweek == i,:]
            typ_days.append(day.groupby(day.index.hour).mean())

        self.fitted = True
        self.typDays = typ_days

    def forecast(self, date):

        if self.fitted:
            date = pd.to_datetime(date).dayofweek
            demand = self.typDays[date].to_numpy().reshape((-1,))
        else:
            with open(r'./data/Ref_Demand.array', 'rb') as file:
                demand = np.load(file).reshape((24,))

        return demand

