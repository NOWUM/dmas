import numpy as np
import pandas as pd


class typFrcst:

    def __init__(self, influx):

        self.influx = influx
        self.fitted = False
        self.mcp = []
        self.index = []

        self.typDays = []

        self.collect = 10
        self.counter = 0

    def collectData(self, date):
        self.index.append(pd.date_range(start=date, periods=24, freq='h'))
        self.mcp.append(self.influx.getDayAheadPrice(date))

    def fitFunction(self):

        timeIndex = pd.DatetimeIndex.append(self.index[0],self.index[1])

        for i in np.arange(2,len(self.index)):
            timeIndex = timeIndex.append(self.index[i])
        df = pd.DataFrame(np.asarray(self.mcp).reshape((-1,1)), timeIndex, ['demand'])

        typDays = []
        for i in range(7):
            day = df.loc[df.index.dayofweek == i,:]
            typDays.append(day.groupby(day.index.hour).mean())

        self.fitted = True
        self.typDays = typDays

    def forecast(self, date):

        if self.fitted:
            date = pd.to_datetime(date).dayofweek
            power_price = self.typDays[date].to_numpy().reshape((-1,))
        else:
            power_price = 25*np.ones(24)

        co = np.ones_like(power_price) * 20                                                 # -- Emission Price     [€/MWh]
        gas = np.ones_like(power_price) * 3                                                 # -- Gas Price          [€/MWh]
        lignite = 1.5                                                                       # -- Lignite Price      [€/MWh]
        coal = 2                                                                            # -- Hard Coal Price    [€/MWh]
        nuc = 1                                                                             # -- nuclear Price      [€/MWh]

        return dict(power=power_price, gas=gas, co=co, lignite=lignite, coal=coal, nuc=nuc)