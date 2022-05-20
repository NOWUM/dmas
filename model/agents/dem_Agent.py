# third party modules
import time as time
import pandas as pd
import numpy as np
from tqdm import tqdm

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
        self.portfolio = DemandPortfolio()
        self.weather_forecast = WeatherForecast(position=dict(lat=self.latitude, lon=self.longitude),
                                                simulation_interface=self.simulation_interface,
                                                weather_interface=self.weather_interface)
        self.price_forecast = PriceForecast(position=dict(lat=self.latitude, lon=self.longitude),
                                            simulation_interface=self.simulation_interface,
                                            weather_interface=self.weather_interface)

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
        pvs = pvs[pvs['ownConsumption'] == 1]
        pvs['type'] = 'solar'
        for system in tqdm(pvs.to_dict(orient='records')):
            self.portfolio.add_energy_system(system)
            demand += system['demandP']
        self.logger.info('Prosumer Photovoltaic added')

        demands = self.infrastructure_interface.get_demand_in_area(self.area)
        household_demand = demands['household'].values[0] * 10**6
        household_demand -= demand

        rlm_demand = demands['business'].values[0] + demands['industry'].values[0] + demands['agriculture'].values[0]
        rlm_demand *= 10**6

        # Construction Standard Consumer H0
        self.portfolio.add_energy_system({'unitID': 'household', 'demandP': household_demand, 'type': 'household'})
        self.logger.info('H0 added')

        # Construction Standard Consumer G0
        #self.portfolio.add_energy_system({'unitID': 'business', 'demandP': business_demand, 'type': 'business'})
        self.logger.info('G0 added')

        # Construction Standard Consumer RLM
        self.portfolio.add_energy_system({'unitID': 'industry', 'demandP': rlm_demand, 'type': 'industry'})
        self.logger.info('RLM added')

        # Construction Standard Consumer agriculture
        #self.portfolio.add_energy_system({'unitID': 'agriculture', 'demandP': agriculture_demand, 'type': 'agriculture'})
        self.logger.info('Agriculture added')

        self.logger.info(f'setup of the agent completed in {np.round(time.time() - start_time,2)} seconds')

    def get_order_book(self, power):
        order_book = {}
        for t in self.portfolio.t:
            if power[t] < 0:
                order_book[t] = dict(type='demand',
                                     hour=t,
                                     order_id=0,
                                     block_id=t,
                                     name=self.name,
                                     price=3000,
                                     volume=power[t])

        df = pd.DataFrame.from_dict(order_book, orient='index')
        if df.empty:
            raise Exception(f'no orders found; order_book: {order_book}')
        return df.set_index(['block_id', 'hour', 'order_id', 'name'])

    def callback(self, ch, method, properties, body):
        super().callback(ch, method, properties, body)

        message = body.decode("utf-8")
        self.date = pd.to_datetime(message.split(' ')[1])
        self.logger.info(f'get command {message}')
        if 'set_capacities' in message:
            self.simulation_interface.set_capacities(self.portfolio,self.area, self.date)
        if 'opt_dayAhead' in message:
            self.optimize_day_ahead()
        if 'result_dayAhead' in message:
            self.post_day_ahead()

    def optimize_day_ahead(self):
        """scheduling for the DayAhead market"""
        self.logger.info('starting day ahead optimization')
        start_time = time.time()

        # Step 1: forecast data and init the model for the coming day
        weather = self.weather_forecast.forecast_for_area(self.date, self.area)
        prices = self.price_forecast.forecast(self.date)

        self.portfolio.set_parameter(self.date, weather.copy(), prices.copy())
        self.portfolio.build_model()
        self.logger.info(f'built model in {np.round(time.time() - start_time,2)} seconds')
        start_time = time.time()
        # Step 2: optimization
        power = self.portfolio.optimize()
        self.logger.info(f'finished day ahead optimization in {np.round(time.time() - start_time,2)} seconds')

        # save optimization results
        self.simulation_interface.set_demand(self.portfolio, 'optimize_dayAhead', self.area, self.date)
        self.simulation_interface.set_generation(self.portfolio, 'optimize_dayAhead', self.area, self.date)

        # Step 3: build orders
        start_time = time.time()
        order_book = self.get_order_book(power)
        self.simulation_interface.set_hourly_orders(order_book)
        self.publish.basic_publish(exchange=self.mqtt_exchange, routing_key='', body=f'{self.name} {self.date.date()}')

        self.logger.info(f'built Orders and send in {np.round(time.time() - start_time, 2)} seconds')

    def post_day_ahead(self):
        """Scheduling after DayAhead Market"""
        self.logger.info('starting day ahead adjustments')
        start_time = time.time()
        # save optimization results
        self.simulation_interface.set_generation(self.portfolio, 'post_dayAhead', self.area, self.date)
        self.simulation_interface.set_demand(self.portfolio, 'post_dayAhead', self.area, self.date)

        self.logger.info(f'finished day ahead adjustments in {np.round(time.time() - start_time, 2)} seconds')