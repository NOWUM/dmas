# third party modules
import time as time
from datetime import timedelta
import pandas as pd
import numpy as np

# model modules
from aggregation.basic_portfolio import PortfolioModel
from agents.basic_Agent import BasicAgent
from interfaces.import_export import EntsoeInfrastructureInterface


class DemEntsoeAgent(BasicAgent):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Portfolio with the corresponding households, trade and industry
        start_time = time.time()
        self.entsoe_interface = EntsoeInfrastructureInterface(self.name, kwargs['entsoe_database_uri'])
        self.portfolio = PortfolioModel(name=self.name)
        self.actual_generation = kwargs.get('entsoe_generation', 'True').lower() == 'true'

        self.logger.info(f'setup of the agent completed in {time.time() - start_time:.2f} seconds')

    def handle_message(self, message):
        if 'set_capacities' in message:
            self.simulation_interface.set_capacities(self.portfolio, self.area, self.date)
        if 'optimize_dayAhead' in message:
            self.optimize_day_ahead()
            return f'optimized_dayAhead {self.name}'
        if 'results_dayAhead' in message:
            self.post_day_ahead()

    def optimize_day_ahead(self):
        """scheduling for the DayAhead market"""
        self.logger.info(f'starting day ahead optimization {self.date}')
        start_time = time.time()

        end = pd.to_datetime(self.date) + timedelta(hours=self.portfolio.T)
        if self.actual_generation:
            demand = self.entsoe_interface.get_generation_in_land('DE', self.date, end).sum(axis=1)
        else:
            demand = self.entsoe_interface.get_demand_in_land('DE', self.date, end)['actual_load']

        empty = pd.DataFrame(index=pd.date_range(start=self.date, freq='h', periods=24), data=np.zeros(24))
        self.portfolio.optimize(self.date, empty.copy(), empty.copy())
        if self.portfolio.dt == 1:
            demand = demand.resample('h').mean()[:self.portfolio.T]
        else:
            raise Exception('not implemented for other frequencies')

        self.portfolio.demand['power'] = np.array(demand)
        self.logger.info(f'finished day ahead optimization in {time.time() - start_time:.2f} seconds')

        # save optimization results
        self.simulation_interface.set_demand(self.portfolio, 'optimize_dayAhead', self.area, self.date)
        self.simulation_interface.set_generation(self.portfolio, 'optimize_dayAhead', self.area, self.date)

        # Step 3: build orders
        start_time = time.time()
        order_book = self.portfolio.get_bid_orders()
        self.simulation_interface.set_hourly_orders(order_book)

        self.logger.info(f'built Orders and send in {time.time() - start_time:.2f} seconds')

    def post_day_ahead(self):
        """Scheduling after DayAhead Market"""
        self.logger.info('starting day ahead adjustments')
        start_time = time.time()
        # save optimization results
        self.simulation_interface.set_generation(self.portfolio, 'post_dayAhead', self.area, self.date)
        self.simulation_interface.set_demand(self.portfolio, 'post_dayAhead', self.area, self.date)

        self.logger.info(f'finished day ahead adjustments in {time.time() - start_time:.2f} seconds')
