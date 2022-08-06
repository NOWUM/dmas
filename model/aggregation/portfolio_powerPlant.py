# third party modules
import numpy as np
from tqdm import tqdm
import logging

# model modules
from systems.generation_powerPlant import PowerPlant
from aggregation.basic_portfolio import PortfolioModel

log = logging.getLogger('power_plant_portfolio')
log.setLevel('INFO')


class PowerPlantPortfolio(PortfolioModel):

    def __init__(self, T=24, date='2020-01-01', steps=np.array([-10, -5, 0, 5, 100, 1e9])/1e3):
        super().__init__(T, date, steps)

    def add_energy_system(self, energy_system):
        model = PowerPlant(T=self.T, steps=self.steps, **energy_system)
        self.capacities[str(energy_system['fuel']).replace('_combined', '')] += energy_system['maxPower'] # [kW]
        self.energy_systems.append(model)

    def update_portfolio_sum():
        try:
            for model in tqdm(self.energy_systems):
                for key, value in model.generation.items():
                    self.generation[str(model.power_plant['fuel']).replace('_combined', '')] += value
                for key, value in model.demand.items():
                    self.demand[key] += value
                for key, value in model.cash_flow.items():
                    self.cash_flow[key] += value
            for key, value in self.generation.items():
                if key != 'total':
                    self.generation['total'] += value

            self.power = self.generation['total'] - self.demand['power']
        except Exception as e:
            log.error(f'error in collecting result: {repr(e)}')


    def optimize(self, date, weather, prices):
        """
        optimize the portfolio for the day ahead market
        :return: time series in [kW] of planned generation
        """
        try:
            self.reset_data()
            for model in self.energy_systems:
                model.optimize(date, weather, prices)
            log.info(f'optimized portfolio')
        except Exception as e:
            log.error(f'error in portfolio optimization: {repr(e)}')

        self.update_portfolio_sum()

        return self.power

    def optimize_post_market(self, committed_power):
        """
        optimize the portfolio after receiving market results
        :return: time series in [kW] of actual generation
        """
        try:
            self.reset_data()
            for model in self.energy_systems:
                model_cp = np.zeros(24)
                filtered_cp = committed_power[committed_power['unitID']==model.name]
                if not filtered_cp.empty:
                    for index, row in filtered_cp.iterrows():
                        model_cp[int(row.hour)] = float(row.volume)

                model.optimize_post_market(model_cp)
            log.info(f'optimized post market results')
        except Exception as e:
            log.error(f'error in portfolio optimization: {repr(e)}')

        self.update_portfolio_sum()

        return self.power

    def get_order_book(self):
        total_order_book = [system.get_orderbook().reset_index() for system in self.energy_systems]

        if len(total_order_book) > 0:
            df = pd.concat(total_order_book, axis=0)
        else:
            df = pd.DataFrame(columns=['block_id', 'hour', 'order_id', 'name',
                                       'price', 'volume', 'link', 'type'])

        df.set_index(['block_id', 'hour', 'order_id', 'name'], inplace=True)

        if not df.loc[df.isna().any(axis=1)].empty:
            self.logger.error('Orderbook has NaN values')
            self.logger.error(df[df.isna()])
        return df