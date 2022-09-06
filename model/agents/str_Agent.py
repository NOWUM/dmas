# third party modules
import time as time
import pandas as pd
from uuid import uuid1


# model modules
from forecasts.price import PriceForecast
from forecasts.weather import WeatherForecast
from forecasts.demand import DemandForecast
from aggregation.portfolio_storage import StrPort
from agents.basic_Agent import BasicAgent


class StrAgent(BasicAgent):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        start_time = time.time()

        self.portfolio = StrPort(name=self.name)

        self.weather_forecast = WeatherForecast(position=dict(lat=self.latitude, lon=self.longitude),
                                                weather_interface=self.weather_interface)
        self.price_forecast = PriceForecast()
        self.demand_forecast = DemandForecast()

        self.storage_names = []

        storages = self.infrastructure_interface.get_water_storage_systems(self.area)
        if storages is not None:
            for _, data in storages.iterrows():
                system = data.to_dict()
                if system['unitID'] in self.storage_names:
                    system['unitID'] = str(uuid1())
                self.storage_names.append(system['unitID'])
                self.portfolio.add_energy_system(system)

        self.logger.info('Storages added')

        self.logger.info(f'setup of the agent completed in {time.time() - start_time:.2f} seconds')

    def handle_message(self, message):
        if 'set_capacities' in message:
            self.simulation_interface.set_capacities(self.portfolio,self.area, self.date)
        elif 'optimize_dayAhead' in message:
            self.optimize_day_ahead()
            return f'optimized_dayAhead {self.name}'
        elif 'results_dayAhead' in message:
            self.post_day_ahead()

    def optimize_day_ahead(self):
        """scheduling for the DayAhead market"""
        self.logger.info(f'dayAhead market scheduling started {self.date}')
        start_time = time.time()

        weather = self.weather_forecast.forecast(self.date, self.area)
        global_weather = self.weather_forecast.forecast(self.date)
        demand = self.demand_forecast.forecast(self.date)
        prices = self.price_forecast.forecast(self.date,weather=global_weather,demand=demand)
        self.logger.info(f'initialize forecast in {time.time() - start_time:.2f} seconds')
        # Step 2: optimization
        self.portfolio.optimize(self.date, weather.copy(), prices.copy())
        self.logger.info(f'finished day ahead optimization in {time.time() - start_time:.2f} seconds')
        # save optimization results
        self.simulation_interface.set_generation(self.portfolio, 'optimize_dayAhead', self.area, self.date)
        self.simulation_interface.set_demand(self.portfolio, 'optimize_dayAhead', self.area, self.date)

        # Step 3: build orders from optimization results
        start_time = time.time()
        order_book = self.portfolio.get_exclusive_orders()
        self.simulation_interface.set_exclusive_orders(order_book)
        # self.simulation_interface.set_orders(order_book, date=self.date, area=self.area)
        self.logger.info(f'built Orders in {time.time() - start_time:.2f} seconds')

    def post_day_ahead(self):
        """Scheduling after DayAhead Market"""
        self.logger.info('starting day ahead adjustments')

        start_time = time.time()
        committed_power = self.simulation_interface.get_exclusive_result(self.storage_names)
        result = self.simulation_interface.get_auction_results(self.date)
        try:
            self.portfolio.optimize_post_market(committed_power, result['price'].values)
        except ValueError as e:
            self.logger.error(repr(e))
        # save optimization results
        self.simulation_interface.set_generation(self.portfolio, 'post_dayAhead', self.area, self.date)
        self.simulation_interface.set_demand(self.portfolio, 'post_dayAhead', self.area, self.date)
        self.simulation_interface.set_cash_flow(self.portfolio, self.area, self.date)

        self.price_forecast.collect_data(date=self.date, market_result=result, weather=self.weather_forecast.get_last())
        self.demand_forecast.collect_data(date=self.date, demand=result['volume'])
        self.forecast_counter -= 1

        if self.forecast_counter == 0:
            self.price_forecast.fit_model()
            self.forecast_counter = 10
            self.logger.info(f'fitted price forecast with R²: {self.price_forecast.score}')