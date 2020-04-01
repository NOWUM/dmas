import numpy as np
import pandas as pd

class typFrcst:

    def __init__(self, influx):

        self.influx = influx
        self.fitted = False
        self.demand = []
        self.index = []

        self.typDays = []

        self.collect = 10
        self.counter = 0

    def collectData(self, date):

        self.index.append(pd.date_range(start=date, periods=24, freq='h'))
        self.demand.append(self.influx.getTotalDemand(date))

    def fitFunction(self):

        timeIndex = pd.DatetimeIndex.append(self.index[0],self.index[1])

        for i in np.arange(2,len(self.index)):
            timeIndex = timeIndex.append(self.index[i])
        df = pd.DataFrame(np.asarray(self.demand).reshape((-1,1)), timeIndex, ['demand'])

        typDays = []
        for i in range(7):
            day = df.loc[df.index.dayofweek == i,:]
            typDays.append(day.groupby(day.index.hour).mean())

        self.fitted = True
        self.typDays = typDays

    def forecast(self, date):

        if self.fitted:
            date = pd.to_datetime(date).dayofweek
            demand = self.typDays[date].to_numpy().reshape((1,-1))
        else:
            demand = 25000 * np.zeros(24)

        return demand