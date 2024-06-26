# third party modules
import time as time
from tqdm import tqdm
import pandas as pd
import numpy as np

# model modules
from forecasts.weather import WeatherForecast
from forecasts.price import PriceForecast
from aggregation.portfolio_demand import DemandPortfolio
from agents.basic_Agent import BasicAgent


class DemAgent(BasicAgent):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Portfolio with the corresponding households, trade and industry
        start_time = time.time()
        self.portfolio = DemandPortfolio(name=self.name)
        self.weather_forecast = WeatherForecast(position=dict(lat=self.latitude, lon=self.longitude),
                                                weather_interface=self.weather_interface)
        # self.price_forecast = PriceForecast()

        demand = 0
        # Construction of the prosumer with photovoltaic and battery
        bats = self.infrastructure_interface.get_solar_storage_systems_in_area(self.area)
        bats['type'] = 'battery'
        for system in tqdm(bats.to_dict(orient='records')):
            self.portfolio.add_energy_system(system)
            demand += system['demandP']
        self.logger.info('Prosumer Photovoltaic and Battery added')

        # Construction consumer with photovoltaic
        pvs = self.infrastructure_interface.get_solar_systems_in_area(self.area, solar_type='roof_top')
        pv_data = pvs[pvs['ownConsumption'] == 1]
        pv_data.loc[:, 'type'] = 'solar'
        for system in tqdm(pv_data.to_dict(orient='records')):
            self.portfolio.add_energy_system(system)
            demand += system['demandP']
        self.logger.info('Prosumer Photovoltaic added')

        demands = self.infrastructure_interface.get_demand_in_area(self.area)
        demands.fillna(0, inplace=True)
        household_demand = demands['household'].values[0] * 1e6
        household_demand -= demand

        business_demand = demands['business'].values[0] * 1e6

        rlm_demand = demands['industry'].values[0] + demands['agriculture'].values[0]
        rlm_demand *= 1e6

        # Construction Standard Consumer H0
        self.portfolio.add_energy_system({'unitID': 'household', 'demandP': household_demand, 'type': 'household'})
        self.logger.info('H0 added')

        # Construction Standard Consumer G0
        self.portfolio.add_energy_system({'unitID': 'business', 'demandP': business_demand, 'type': 'business'})
        self.logger.info('G0 added')

        # Construction Standard Consumer RLM
        self.portfolio.add_energy_system({'unitID': 'industry', 'demandP': rlm_demand, 'type': 'industry'})
        self.logger.info('RLM added')

        # Construction Standard Consumer agriculture
        # self.portfolio.add_energy_system({'unitID': 'agriculture', 'demandP': agriculture_demand, 'type': 'agriculture'})
        # self.logger.info('Agriculture added')

        self.portfolio.add_unique_systems()

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
        # Step 1: forecast data and init the model for the coming day
        weather = self.weather_forecast.forecast(self.date, local=self.area)
        # prices = self.price_forecast.forecast(self.date)
        prices = pd.DataFrame(index=pd.date_range(start=self.date, freq='h', periods=24), data=np.zeros(24))
        self.logger.info(f'built model in {time.time() - start_time:.2f} seconds')

        start_time = time.time()
        # Step 2: optimization
        power = self.portfolio.optimize(self.date, weather.copy(), prices.copy())
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
