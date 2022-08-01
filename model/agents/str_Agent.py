# third party modules
import time as time
import pandas as pd
import numpy as np
from websockets import WebSocketClientProtocol as wsClientPrtl


# model modules
from aggregation.portfolio_storage import StrPort
from agents.basic_Agent import BasicAgent


class StrAgent(BasicAgent):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        start_time = time.time()

        self.portfolio = StrPort(T=24)

        self.max_volume = 0
        # Construction storages
        storages = self.infrastructure_interface.get_water_storage_systems(self.area)
        if storages is not None:
            for _, data in storages.iterrows():
                self.portfolio.add_energy_system(data.to_dict())

        self.logger.info('Storages added')

        df = pd.DataFrame(index=[pd.to_datetime(self.date)], data=self.portfolio.capacities)
        df['agent'] = self.name
        df.to_sql(name='installed capacities', con=self.simulation_database, if_exists='replace')

        self.logger.info(f'setup of the agent completed in {time.time() - start_time:.2f} seconds')

    async def handle_message(self, message):
        if 'set_capacities' in message:
            self.simulation_interface.set_capacities(self.portfolio,self.area, self.date)
        if 'optimize_dayAhead' in message:
            self.optimize_day_ahead()
            await ws.send(f'optimized_dayAhead {self.name}')
        if 'results_dayAhead' in message:
            self.post_day_ahead()

    def optimize_day_ahead(self):
        """scheduling for the DayAhead market"""
        self.logger.info('dayAhead market scheduling started')


    def post_day_ahead(self):
        """Scheduling after DayAhead Market"""
        self.logger.info('starting day ahead adjustments')

