from forecasts.basic_forecast import BasicForecast
import numpy as np
import pandas as pd


with open(r'./forecasts/data/default_demand.pkl', 'rb') as file:
    default_demand = np.load(file).reshape((24,))


class DemandForecast(BasicForecast):

    def __init__(self, position, simulation_interface, weather_interface):
        super().__init__(position, simulation_interface, weather_interface)
        self.model = {i: [] for i in range(7)}

    def collect_data(self, date):
        demand = self.market.get_auction_results(date)
        values = demand['volume'].to_numpy()
        if len(demand.index) < 24:
            raise Exception('No Results from market available')
        for i in range(24):
            self.input.append(demand.index[i])
            self.output.append(values[i])

    def fit_model(self):
        df = pd.DataFrame(index=self.input, data={'demand': self.output})
        for i in range(7):
            day = df.loc[[d.dayofweek == i for d in df.index], :]
            self.model[i] = day.groupby(day.index.hour).mean()

    def forecast(self, date):
        if self.fitted:
            demand = self.model[int(pd.to_datetime(date).dayofweek)]
            demand.index = pd.date_range(start=date, freq='h', periods=len(demand))
            demand.index.name = 'time'
        else:
            demand = pd.DataFrame(index=pd.date_range(start=date, freq='h', periods=len(default_demand)),
                                  data={'demand': default_demand})
            demand.index.name = 'time'

        return demand

