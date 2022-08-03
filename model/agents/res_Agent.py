# third party modules
import time as time
import pandas as pd
import numpy as np
from tqdm import tqdm
from websockets import WebSocketClientProtocol as wsClientPrtl

# model modules
from forecasts.weather import WeatherForecast
from forecasts.price import PriceForecast
from aggregation.portfolio_renewable import RenewablePortfolio
from agents.basic_Agent import BasicAgent


class ResAgent(BasicAgent):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Development of the portfolio with the corresponding ee-systems
        start_time = time.time()
        self.portfolio_eeg = RenewablePortfolio()
        self.portfolio_mrk = RenewablePortfolio()

        self.weather_forecast = WeatherForecast(position=dict(lat=self.latitude, lon=self.longitude),
                                                simulation_interface=self.simulation_interface,
                                                weather_interface=self.weather_interface)
        self.price_forecast = PriceForecast(position=dict(lat=self.latitude, lon=self.longitude),
                                            simulation_interface=self.simulation_interface,
                                            weather_interface=self.weather_interface)

        # Construction Wind energy
        wind_data = self.infrastructure_interface.get_wind_turbines_in_area(self.area, wind_type='on_shore')
        wind_data['type'] = 'wind'
        if 'windFarm' not in wind_data.columns:
            wind_data['windFarm'] = ''
        for wind_farm in tqdm(wind_data['windFarm'].unique()):
            if wind_farm != 'x':
                turbines = [row.to_dict() for _, row in wind_data[wind_data['windFarm'] == wind_farm].iterrows()]
                max_power = sum([turbine['maxPower'] for turbine in turbines])
                self.portfolio_mrk.add_energy_system({'unitID': wind_farm, 'turbines': turbines, 'type': 'wind',
                                                      'maxPower': max_power})
            else:
                for _, turbine in wind_data[wind_data['windFarm'] == wind_farm].iterrows():
                    self.portfolio_mrk.add_energy_system({'unitID': turbine['unitID'], 'turbines': turbine.to_dict(),
                                                          'type': 'wind', 'maxPower': turbine['maxPower']})
        self.logger.info('Wind Power Plants added')

        # Construction of the solar power plant
        pv_1 = self.infrastructure_interface.get_solar_systems_in_area(self.area, solar_type='free_area')
        pv_2 = self.infrastructure_interface.get_solar_systems_in_area(self.area, solar_type='other')
        pvs = pd.concat([pv_1, pv_2])
        pvs['type'] = 'solar'
        for system in tqdm(pvs[pvs['eeg'] == 1].to_dict(orient='records')):
            self.portfolio_eeg.add_energy_system(system)
        for system in tqdm(pvs[pvs['eeg'] == 0].to_dict(orient='records')):
            self.portfolio_mrk.add_energy_system(system)
        self.logger.info('Solar power plant added')
        # Freiflächen-PV-Anlage

        # Construction of the pv systems (h0)
        pv_data = self.infrastructure_interface.get_solar_systems_in_area(self.area, solar_type='roof_top')
        pv_data['type'] = 'solar'
        for system in tqdm(pvs[pvs['ownConsumption'] == 0].to_dict(orient='records')):
            self.portfolio_eeg.add_energy_system(system)

        self.logger.info('Household PV added')
        self.pv_data = pv_data

        # Construction Run River
        run_river_data = self.infrastructure_interface.get_run_river_systems_in_area(self.area)
        run_river_data['type'] = 'water'
        for system in tqdm(run_river_data.to_dict(orient='records')):
            self.portfolio_eeg.add_energy_system(system)
        self.logger.info('Run River Power Plants added')

        # Construction Biomass
        bio_mass_data = self.infrastructure_interface.get_biomass_systems_in_area(self.area)
        bio_mass_data['type'] = 'bio'
        for system in tqdm(bio_mass_data.to_dict(orient='records')):
            self.portfolio_eeg.add_energy_system(system)
        self.logger.info('Biomass Power Plants added')

        self.logger.info(f'setup of the agent completed in {time.time() - start_time:.2f} seconds')

    def get_order_book(self, power, type='eeg'):
        order_book = {}
        for t in np.arange(len(power)):
            if type == 'eeg' and power[t] > 0.5:
                order_book[t] = dict(type='generation',
                                     hour=t,
                                     block_id=t,
                                     order_id=0,
                                     name=self.name + '_eeg',
                                     price=-500/1e3, # €/kWh min eex bid
                                     # lower limit of DA auction
                                     # https://www.epexspot.com/sites/default/files/2022-05/22-05-23_TradingBrochure.pdf
                                     volume=power[t])
            if type == 'mrk' and power[t] > 0.5:
                order_book[t] = dict(type='generation',
                                     block_id=t,
                                     hour=t,
                                     order_id=0,
                                     name=self.name + '_mrk',
                                     # TODO better values
                                     price=1e-4, # [€/kWh] # this is relevant for negative prices §51 EEG
                                     volume=power[t])

        df = pd.DataFrame.from_dict(order_book, orient='index')
        if df.empty:
            df = pd.DataFrame(columns=['type', 'block_id', 'hour', 'order_id', 'name', 'price', 'volume'])
        df = df.set_index(['block_id', 'hour', 'order_id', 'name'])

        return df

    def handle_message(self, message):
        if 'set_capacities' in message:
            self.simulation_interface.set_capacities([self.portfolio_mrk, self.portfolio_eeg], self.area, self.date)
        if 'optimize_dayAhead' in message:
            self.optimize_day_ahead()
            return f'optimized_dayAhead {self.name}'
        if 'results_dayAhead' in message:
            self.post_day_ahead()

    def optimize_day_ahead(self):
        """Scheduling before DayAhead Market"""
        self.logger.info(f'dayAhead market scheduling started {self.date}')
        start_time = time.time()

        # Step 1: forecast data data and init the model for the coming day
        weather = self.weather_forecast.forecast_for_area(self.date, self.area)
        prices = self.price_forecast.forecast(self.date)

        self.portfolio_eeg.set_parameter(self.date, weather.copy(),  prices.copy())
        self.portfolio_eeg.build_model()

        self.portfolio_mrk.set_parameter(self.date, weather.copy(),  prices.copy())
        self.portfolio_mrk.build_model()
        self.logger.info(f'built model in {time.time() - start_time:.2f} seconds')
        start_time = time.time()
        # Step 2: optimization
        power_eeg = self.portfolio_eeg.optimize()
        power_mrk = self.portfolio_mrk.optimize()
        self.logger.info(f'finished day ahead optimization in {time.time() - start_time:.2f} seconds')

        # save optimization results
        self.simulation_interface.set_generation([self.portfolio_mrk, self.portfolio_eeg], step='optimize_dayAhead',
                                                 area=self.area, date=self.date)
        self.simulation_interface.set_demand([self.portfolio_mrk, self.portfolio_eeg], step='optimize_dayAhead',
                                             area=self.area, date=self.date)

        # Step 3: build orders from optimization results
        start_time = time.time()
        order_book = self.get_order_book(power_eeg, type='eeg')
        self.simulation_interface.set_hourly_orders(order_book)
        order_book = self.get_order_book(power_mrk, type='mrk')
        self.simulation_interface.set_hourly_orders(order_book)

        self.logger.info(f'built Orders and send in {time.time() - start_time:.2f} seconds')

    def post_day_ahead(self):
        """Scheduling after DayAhead Market"""
        self.logger.info(f'starting day ahead adjustments {self.date}')
        start_time = time.time()

        market_result = self.simulation_interface.get_hourly_result(self.name)
        power = np.zeros(self.portfolio_eeg.T)
        for index, row in market_result.iterrows():
            power[int(row.hour)] = float(row.volume)

        # -> fist EEG (power > power_eeg -> no adjustments)
        self.portfolio_eeg.committed = power
        power_eeg = self.portfolio_eeg.optimize()
        # -> second MARKET ((power - power_eeg) < power_mrk -> adjustments)
        self.portfolio_mrk.committed = power - power_eeg
        self.portfolio_mrk.optimize()

        # save optimization results
        self.simulation_interface.set_generation([self.portfolio_mrk, self.portfolio_eeg], 'post_dayAhead', self.area, self.date)
        self.simulation_interface.set_demand([self.portfolio_mrk, self.portfolio_eeg], 'post_dayAhead', self.area, self.date)

        self.logger.info(f'finished day ahead adjustments in {time.time() - start_time:.2f} seconds')
