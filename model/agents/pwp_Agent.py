# third party modules
import time

from tqdm import tqdm

# model modules
from forecasts.price import PriceForecast
from forecasts.weather import WeatherForecast
from forecasts.demand import DemandForecast
from aggregation.portfolio_powerPlant import PowerPlantPortfolio
from agents.basic_Agent import BasicAgent


class PwpAgent(BasicAgent):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        start_time = time.time()

        self.portfolio: PowerPlantPortfolio = PowerPlantPortfolio(name=self.name)

        self.weather_forecast = WeatherForecast(position=dict(lat=self.latitude, lon=self.longitude),
                                                weather_interface=self.weather_interface)
        self.price_forecast = PriceForecast(use_historic_data=False, use_real_data=False)
        self.demand_forecast = DemandForecast()

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
        local_weather = self.weather_forecast.forecast(date=self.date, steps=48, local=self.area)
        global_weather = self.weather_forecast.forecast(self.date, steps=48)
        demand = self.demand_forecast.forecast(self.date, steps=48)
        prices = self.price_forecast.forecast(date=self.date, weather=global_weather, demand=demand, steps=48)

        return local_weather, prices

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

        self.price_forecast.collect_data(date=self.date, market_result=result, weather=self.weather_forecast.get_last())
        self.demand_forecast.collect_data(date=self.date, demand=result['volume'])
        self.forecast_counter -= 1

        if self.forecast_counter == 0:
            self.price_forecast.fit_model()
            self.forecast_counter = 10
            self.logger.info('fitted price forecast')

        self.logger.info(f'finished day ahead adjustments in {time.time() - start_time:.2f} seconds')
