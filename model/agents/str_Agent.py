# third party modules
import time as time
import pandas as pd
import numpy as np

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

        self.logger.info(f'setup of the agent completed in {np.round(time.time() - start_time,2)} seconds')

    def callback(self, ch, method, properties, body):
        super().callback(ch, method, properties, body)

        message = body.decode("utf-8")

        self.date = pd.to_datetime(message.split(' ')[1])
        self.simulation_interface.date = self.date

        if 'set_capacities' in message:
            self.simulation_interface.set_capacities(self.portfolio)
        if 'opt_dayAhead' in message:
            self.optimize_day_ahead()
        if 'result_dayAhead' in message:
            self.post_day_ahead()

    def optimize_day_ahead(self):
        """scheduling for the DayAhead market"""
        self.logger.info('dayAhead market scheduling started')

        start_time = time.time()
        self.publish.basic_publish(exchange=self.mqtt_exchange, routing_key='', body=f'{self.name} {self.date.date()}')

        self.logger.info(f'built Orders in {np.round(time.time() - start_time, 2)} seconds')

    def post_day_ahead(self):
        """Scheduling after DayAhead Market"""
        start_time = time.time()

        self.logger.info('starting day ahead adjustments')

        self.logger.info(f'finished day ahead adjustments in {np.round(time.time() - start_time, 2)} seconds')
