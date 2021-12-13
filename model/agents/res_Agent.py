# third party modules
import time as time
import pandas as pd
import numpy as np
from tqdm import tqdm


# model modules
from forecasts.weather import WeatherForecast
from forecasts.price import PriceForecast
from aggregation.portfolio_renewable import RenewablePortfolio
from agents.basic_Agent import BasicAgent



class ResAgent(BasicAgent):

    def __init__(self, date, plz, agent_type, connect,  infrastructure_source, infrastructure_login, *args, **kwargs):
        super().__init__(date, plz, agent_type, connect, infrastructure_source, infrastructure_login)
        # Development of the portfolio with the corresponding ee-systems
        self.logger.info('starting the agent')
        start_time = time.time()
        self.portfolio_eeg = RenewablePortfolio()
        self.portfolio_mrk = RenewablePortfolio()

        self.weather_forecast = WeatherForecast(position=dict(lat=self.latitude, lon=self.longitude))
        self.price_forecast = PriceForecast(position=dict(lat=self.latitude, lon=self.longitude))

        # Construction Wind energy
        wind_data = self.infrastructure_interface.get_wind_turbines_in_area(area=plz, wind_type='on_shore')
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

        # Construction of the pv systems (free area)
        pv_1 = self.infrastructure_interface.get_solar_systems_in_area(area=plz, solar_type='free_area')
        pv_2 = self.infrastructure_interface.get_solar_systems_in_area(area=plz, solar_type='other')
        pvs = pd.concat([pv_1, pv_2])
        pvs['type'] = 'solar'
        for system in tqdm(pvs[pvs['eeg'] == 1].to_dict(orient='records')):
            self.portfolio_eeg.add_energy_system(system)
        for system in tqdm(pvs[pvs['eeg'] == 0].to_dict(orient='records')):
            self.portfolio_mrk.add_energy_system(system)
        self.logger.info('Free Area PV added')

        # Construction of the pv systems (h0)
        pv_data = self.infrastructure_interface.get_solar_systems_in_area(area=plz, solar_type='roof_top')
        pv_data['type'] = 'solar'
        for system in tqdm(pvs[pvs['ownConsumption'] == 0].to_dict(orient='records')):
            self.portfolio_eeg.add_energy_system(system)

        self.logger.info('Household PV added')
        self.pv_data = pv_data

        # Construction Run River
        run_river_data = self.infrastructure_interface.get_run_river_systems_in_area(area=plz)
        run_river_data['type'] = 'water'
        for system in tqdm(run_river_data.to_dict(orient='records')):
            self.portfolio_eeg.add_energy_system(system)
        self.logger.info('Run River Power Plants added')

        # Construction Biomass
        bio_mass_data = self.infrastructure_interface.get_biomass_systems_in_area(area=plz)
        bio_mass_data['type'] = 'bio'
        for system in tqdm(bio_mass_data.to_dict(orient='records')):
            self.portfolio_eeg.add_energy_system(system)
        self.logger.info('Biomass Power Plants added')

        df_mrk = pd.DataFrame(index=[pd.to_datetime(self.date)], data=self.portfolio_mrk.capacities)
        df_eeg = pd.DataFrame(index=[pd.to_datetime(self.date)], data=self.portfolio_mrk.capacities)
        for col in df_mrk.columns:
            df_mrk[col] += df_eeg[col]
        df_mrk['agent'] = self.name
        df_mrk.index.name = 'time'
        df_mrk.to_sql(name='capacities', con=self.simulation_database, if_exists='append')

        self.logger.info(f'setup of the agent completed in {np.round(time.time() - start_time,2)} seconds')

    def callback(self, ch, method, properties, body):
        super().callback(ch, method, properties, body)

        message = body.decode("utf-8")
        self.date = pd.to_datetime(message.split(' ')[1])

        if 'opt_dayAhead' in message:
            self.optimize_day_ahead()
        if 'result_dayAhead' in message:
            self.post_day_ahead()

    def optimize_day_ahead(self):
        """Scheduling before DayAhead Market"""
        self.logger.info(f'DayAhead market scheduling started {self.date}')
        start_time = time.time()

        # Step 1: forecast data data and init the model for the coming day
        weather = self.weather_forecast.forecast_for_area(self.date, int(self.plz/10))
        prices = self.price_forecast.forecast(self.date)

        self.portfolio_eeg.set_parameter(self.date, weather.copy(),  prices.copy())
        self.portfolio_eeg.build_model()

        self.portfolio_mrk.set_parameter(self.date, weather.copy(),  prices.copy())
        self.portfolio_mrk.build_model()
        self.logger.info(f'built model in {np.round(time.time() - start_time,2)} seconds')
        start_time = time.time()
        # Step 2: optimization
        power_eeg = self.portfolio_eeg.optimize()
        power_mrk = self.portfolio_mrk.optimize()
        self.logger.info(f'Finished day ahead optimization in {np.round(time.time() - start_time, 2)} seconds')
        # Step 3: save optimization results
        df_mrk = pd.DataFrame(data=self.portfolio_mrk.demand, index=pd.date_range(start=self.date, freq='h', periods=24))
        df_eeg = pd.DataFrame(data=self.portfolio_eeg.demand, index=pd.date_range(start=self.date, freq='h', periods=24))

        for col in df_mrk.columns:
            df_mrk[col] += df_eeg[col]

        df_mrk['step'] = 'optimize_day_ahead'
        df_mrk['agent'] = self.name
        df_mrk.index.name = 'time'
        df_mrk.to_sql('demand', con=self.simulation_database, if_exists='append')

        df_mrk = pd.DataFrame(data=self.portfolio_mrk.generation, index=pd.date_range(start=self.date, freq='h', periods=24))
        df_eeg = pd.DataFrame(data=self.portfolio_eeg.generation, index=pd.date_range(start=self.date, freq='h', periods=24))

        for col in df_mrk.columns:
            df_mrk[col] += df_eeg[col]

        df_mrk['step'] = 'optimize_day_ahead'
        df_mrk['agent'] = self.name
        df_mrk.index.name = 'time'
        df_mrk.to_sql('generation', con=self.simulation_database, if_exists='append')

        # Step 4: build orders from optimization results
        start_time = time.time()
        orders_mrk = {t: {'type': 'generation', 'block': f'MRK{t}', 'hour': t, 'order': 0, 'name': self.name, 'price': 0,
                          'volume': power_mrk[t], 'link': -1} for t in self.portfolio_mrk.t}
        orders_eeg = {t: {'type': 'generation', 'block': f'EEG{t}', 'hour': t, 'order': 0, 'name': self.name, 'price': -500,
                          'volume': power_eeg[t], 'link': -1} for t in self.portfolio_mrk.t}

        df_mrk = pd.DataFrame.from_dict(orders_mrk, orient='index')
        df_mrk = df_mrk.set_index(['block', 'hour', 'order', 'name'])

        df_eeg = pd.DataFrame.from_dict(orders_eeg, orient='index')
        df_eeg = df_eeg.set_index(['block', 'hour', 'order', 'name'])
        df = pd.concat([df_mrk, df_eeg])
        df.to_sql('orders', con=self.simulation_database, if_exists='append')

        self.logger.info(f'Built Orders in {np.round(time.time() - start_time, 2)} seconds')

        self.publish.basic_publish(exchange=self.exchange_name, routing_key='', body=f'grid_calc {self.date}')

    def post_day_ahead(self):
        """Scheduling after DayAhead Market"""
        self.logger.info('After DayAhead market scheduling started')

        # Step 6: get market results and adjust generation an strategy
        # -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()

        # query the DayAhead results
        ask = self.connections['influxDB'].get_ask_da(self.date, self.name)            # volume to buy
        bid = self.connections['influxDB'].get_bid_da(self.date, self.name)            # volume to sell
        prc = self.connections['influxDB'].get_prc_da(self.date)                       # market clearing price
        profit = (ask - bid) * prc

        self.week_price_list.remember_price(prcToday=prc)

        # adjust power generation
        self.portfolio.build_model(response=ask - bid)
        _ = self.portfolio.optimize()

        self.performance['adjustResult'] = np.round(tme.time() - start_time, 3)

        # Step 7: save adjusted results in influxdb
        # -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()

        df = pd.concat([pd.DataFrame.from_dict(self.portfolio.generation),
                        pd.DataFrame(data=dict(profit=profit))], axis=1)
        df.index = pd.date_range(start=self.date, freq='60min', periods=len(df))
        self.connections['influxDB'].save_data(df, 'Areas', dict(typ=self.typ, agent=self.name, area=self.plz,
                                                                 timestamp='post_dayAhead'))

        self.performance['saveResult'] = np.round(tme.time() - start_time, 3)

        self.logger.info('After DayAhead market adjustment completed')
        print('After DayAhead market adjustment completed:', self.name)
        self.logger.info('Next day scheduling started')

        # Step 8: retrain forecast methods and learning algorithm
        # -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()

        # No Price Forecast  used actually
        self.week_price_list.put_price()

        df = pd.DataFrame(index=[pd.to_datetime(self.date)], data=self.portfolio.capacities)
        self.connections['influxDB'].save_data(df, 'Areas', dict(typ=self.typ, agent=self.name, area=self.plz))

        self.performance['nextDay'] = np.round(tme.time() - start_time, 3)

        df = pd.DataFrame(data=self.performance, index=[self.date])
        self.connections['influxDB'].save_data(df, 'Performance', dict(typ=self.typ, agent=self.name, area=self.plz))

        self.logger.info('Next day scheduling completed')