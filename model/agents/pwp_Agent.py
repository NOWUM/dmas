# third party modules
import time

import pandas as pd
from tqdm import tqdm

# model modules
from forecasts.price import PriceForecast
from forecasts.weather import WeatherForecast
from aggregation.portfolio_powerPlant import PowerPlantPortfolio
from agents.basic_Agent import BasicAgent


class PwpAgent(BasicAgent):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        start_time = time.time()

        self.portfolio: PowerPlantPortfolio = PowerPlantPortfolio(name=self.name)

        self.weather_forecast = WeatherForecast(position=dict(lat=self.latitude, lon=self.longitude),
                                                simulation_interface=self.simulation_interface,
                                                weather_interface=self.weather_interface)
        self.price_forecast = PriceForecast(position=dict(lat=self.latitude, lon=self.longitude),
                                            simulation_interface=self.simulation_interface,
                                            weather_interface=self.weather_interface)
        self.forecast_counter = 10

        self.pwp_names = []

        for fuel in tqdm(['lignite', 'coal', 'gas', 'nuclear']):
            power_plants = self.infrastructure_interface.get_power_plant_in_area(self.area, fuel_type=fuel)
            if not power_plants.empty:
                for system in power_plants.to_dict(orient='records'):
                    self.pwp_names.append(system['unitID'])
                    self.portfolio.add_energy_system(system)

        # Construction power plants
        self.logger.info('Power Plants added')

        self.logger.info(f'setup of the agent completed in {time.time() - start_time:.2f} seconds')

    def handle_message(self, message):
        if 'set_capacities' in message:
            self.simulation_interface.set_capacities(self.portfolio, self.area, self.date)
        if 'optimize_dayAhead' in message:
            self.optimize_day_ahead()
            return f'optimized_dayAhead {self.name}'
        if 'results_dayAhead' in message:
            self.post_day_ahead()

    def _initialize_parameters(self):
        # Step 1: forecast data and init the model for the coming day
        weather = self.weather_forecast.forecast_for_area(self.date, self.area)
        prices = self.price_forecast.forecast(self.date)
        # use tomorrows price forecast also for aftertomorrow
        prices = pd.concat([prices, prices.copy()])
        prices.index = pd.date_range(start=self.date, freq='h', periods=48)

        return weather, prices

    def optimize_day_ahead(self):
        """scheduling for the DayAhead market"""
        self.logger.info(f'dayAhead market scheduling started {self.date}')
        start_time = time.time()

        weather, prices = self._initialize_parameters()
        self.logger.info(f'initialize forecast in {time.time() - start_time:.2f} seconds')
        # Step 2: optimization
        self.portfolio.optimize(self.date, weather.copy(), prices.copy())
        self.logger.info(f'finished day ahead optimization in {time.time() - start_time:.2f} seconds')

        # save optimization results
        self.simulation_interface.set_generation(self.portfolio, 'optimize_dayAhead', self.area, self.date)
        self.simulation_interface.set_demand(self.portfolio, 'optimize_dayAhead', self.area, self.date)

        # Step 3: build orders from optimization results
        start_time = time.time()
        order_book = self.portfolio.get_ask_orders()
        self.simulation_interface.set_linked_orders(order_book)
        self.simulation_interface.set_orders(order_book, date=self.date, area=self.area)
        self.logger.info(f'built Orders in {time.time() - start_time:.2f} seconds')

    def post_day_ahead(self):
        """Scheduling after DayAhead Market"""
        self.logger.info('starting day ahead adjustments')

        start_time = time.time()

        committed_power = self.simulation_interface.get_linked_result(self.pwp_names)
        result = self.simulation_interface.get_auction_results(self.date)
        try:
            self.portfolio.optimize_post_market(committed_power, result['price'].values)
        except ValueError as e:
            self.logger.error(repr(e))
        except Exception:
            self.logger.exception('Error in PostMarket')
        # save optimization results
        self.simulation_interface.set_generation(self.portfolio, 'post_dayAhead', self.area, self.date)
        self.simulation_interface.set_demand(self.portfolio, 'post_dayAhead', self.area, self.date)
        self.simulation_interface.set_cash_flow(self.portfolio, self.area, self.date)

        self.weather_forecast.collect_data(self.date)
        self.price_forecast.collect_data(self.date)
        self.forecast_counter -= 1

        if self.forecast_counter == 0:
            self.price_forecast.fit_model()
            self.forecast_counter = 10
            self.logger.info(f'fitted price forecast with R²: {self.price_forecast.score}')

        self.logger.info(f'finished day ahead adjustments in {time.time() - start_time:.2f} seconds')
