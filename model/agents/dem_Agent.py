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

    def __init__(self, date, plz, agent_type, connect,  infrastructure_source, infrastructure_login, *args, **kwargs):
        super().__init__(date, plz, agent_type, connect, infrastructure_source, infrastructure_login)
        # Portfolio with the corresponding households, trade and industry
        self.logger.info('starting the agent')
        start_time = time.time()
        self.portfolio = DemandPortfolio()
        self.weather_forecast = WeatherForecast(position=dict(lat=self.latitude, lon=self.longitude))
        self.price_forecast = PriceForecast(position=dict(lat=self.latitude, lon=self.longitude))

        demand = 0
        # Construction of the prosumer with photovoltaic and battery
        bats = self.infrastructure_interface.get_solar_storage_systems_in_area(area=plz)
        bats['type'] = 'battery'
        for system in tqdm(bats.to_dict(orient='records')):
            self.portfolio.add_energy_system(system)
            demand += system['demandP'] / 10**9
        self.logger.info('Prosumer Photovoltaic and Battery added')

        # Construction consumer with photovoltaic
        pvs = self.infrastructure_interface.get_solar_systems_in_area(area=plz, solar_type='roof_top')
        pvs = pvs[pvs['ownConsumption'] == 1]
        pvs['maxPower'] *= 1000
        pvs['type'] = 'solar'
        for system in tqdm(pvs.to_dict(orient='records')):
            self.portfolio.add_energy_system(system)
            demand += system['demandP'] / 10**9
        self.logger.info('Prosumer Photovoltaic added')

        total_demand, household, industry_business = self.infrastructure_interface.get_demand_in_area(area=plz)
        household_demand = (total_demand * household - demand) * 10**9
        business_demand = total_demand * industry_business * 0.5 * 10**9
        industry_demand = business_demand

        # Construction Standard Consumer H0
        self.portfolio.add_energy_system({'unitID': 'household', 'demandP': household_demand, 'type': 'household'})
        self.logger.info('H0 added')

        # Construction Standard Consumer G0
        self.portfolio.add_energy_system({'unitID': 'business', 'demandP': business_demand, 'type': 'business'})
        self.logger.info('G0 added')

        # Construction Standard Consumer RLM
        self.portfolio.add_energy_system({'unitID': 'industry', 'demandP': industry_demand, 'type': 'industry'})
        self.logger.info('RLM added')



        self.logger.info(f'setup of the agent completed in {np.round(time.time() - start_time,2)} seconds')

    def callback(self, ch, method, properties, body):
        super().callback(ch, method, properties, body)

        message = body.decode("utf-8")
        self.date = pd.to_datetime(message.split(' ')[1])

        if 'set_capacities' in message:
            self.set_capacities()
        if 'opt_dayAhead' in message:
            self.optimize_day_ahead()
        if 'result_dayAhead' in message:
            self.post_day_ahead()

    def set_capacities(self):
        df = pd.DataFrame(index=[pd.to_datetime(self.date)], data=self.portfolio.capacities)
        df['agent'] = self.name
        df.index.name = 'time'
        df.to_sql(name='capacities', con=self.simulation_database, if_exists='append')

    def optimize_day_ahead(self):
        """scheduling for the DayAhead market"""
        self.logger.info('starting day ahead optimization')
        start_time = time.time()

        # Step 1: forecast data data and init the model for the coming day
        weather = self.weather_forecast.forecast_for_area(self.date, int(self.plz/10))
        prices = self.price_forecast.forecast(self.date)

        self.portfolio.set_parameter(self.date, weather.copy(), prices.copy())
        self.portfolio.build_model()
        self.logger.info(f'built model in {np.round(time.time() - start_time,2)} seconds')
        start_time = time.time()
        # Step 2: optimization
        power = self.portfolio.optimize()
        self.logger.info(f'finished day ahead optimization in {np.round(time.time() - start_time,2)} seconds')

        # save optimization results
        df = pd.DataFrame(data=self.portfolio.demand, index=pd.date_range(start=self.date, freq='h', periods=24))
        df['step'] = 'optimize_day_ahead'
        df['agent'] = self.name
        df.index.name = 'time'
        df.to_sql('demand', con=self.simulation_database, if_exists='append')

        df = pd.DataFrame(data=self.portfolio.generation, index=pd.date_range(start=self.date, freq='h', periods=24))
        df['step'] = 'optimize_day_ahead'
        df['agent'] = self.name
        df.index.name = 'time'
        df.to_sql('generation', con=self.simulation_database, if_exists='append')

        # Step 3: build orders
        start_time = time.time()
        orders = {t: {'type': 'demand', 'block_id': t, 'hour': t, 'order_id': 0, 'name': self.name, 'price': 3000,
                      'volume': power[t], 'link': -1} for t in self.portfolio.t}
        df = pd.DataFrame.from_dict(orders, orient='index')
        df = df.set_index(['block_id', 'hour', 'order_id', 'name'])
        df.to_sql('orders', con=self.simulation_database, if_exists='append')
        self.publish.basic_publish(exchange=self.exchange_name, routing_key='', body=f'{self.name} {self.date.date()}')

        self.logger.info(f'built Orders and send in {np.round(time.time() - start_time, 2)} seconds')


    def post_day_ahead(self):
        """Scheduling after DayAhead Market"""
        self.logger.info('starting day ahead adjustments')

        # Step 1: get market results
        start_time = time.time()

        # query the DayAhead results
        agent_volume = pd.read_sql(f"Select hour, sum(volume) from orders where name = '{self.name}' group by hour",
                                   self.simulation_database)

        print(agent_volume)

        start_date = self.date.date()
        end_date = (self.date + pd.DateOffset(days=1)).date()
        prices = pd.read_sql(f"Select time, price from market where time >= '{start_date}' and time < '{end_date}'",
                                   self.simulation_database)
        print(prices)