# third party modules
import time as tme
import pandas as pd
import numpy as np

# model modules
from aggregation.portfolio_renewable import RenewablePortfolio
from agents.client_Agent import agent as basicAgent


class ResAgent(basicAgent):

    def __init__(self, date, plz, agent_type, mqtt_exchange, connect,  infrastructure_source, infrastructure_login):
        super().__init__(date, plz, agent_type, mqtt_exchange, connect, infrastructure_source, infrastructure_login)
        # Development of the portfolio with the corresponding ee-systems
        self.logger.info('starting the agent')
        start_time = tme.time()
        self.eeg_portfolio = RenewablePortfolio()
        self.mrk_portfolio = RenewablePortfolio()

        # Construction Wind energy
        wind_data = self.infrastructure_interface.get_wind_turbines_in_area(area=plz, wind_type='on_shore')
        wind_data['type'] = 'wind'
        for wind_farm in wind_data['windFarm'].unique():
            if wind_farm != 'x':
                turbines = [row.to_dict() for _, row in wind_data[wind_data['windFarm'] == wind_farm].iterrows()]
                self.mrk_portfolio.add_energy_system({'unitID': wind_farm, 'turbines': turbines, 'type': 'wind'})
            else:
                for _, turbine in wind_data[wind_data['windFarm'] == wind_farm].iterrows():
                    self.mrk_portfolio.add_energy_system({'unitID': turbine['unitID'], 'turbines': turbine.to_dict(),
                                                          'type': 'wind'})
        self.logger.info('Wind Power Plants added')

        # Construction of the pv systems (free area)
        pv_1 = self.infrastructure_interface.get_solar_systems_in_area(area=plz, solar_type='free_area')
        pv_2 = self.infrastructure_interface.get_solar_systems_in_area(area=plz, solar_type='other')
        pv_data = pd.concat([pv_1, pv_2])
        pv_data['type'] = 'solar'
        pv_data['open_space'] = True
        for _, system_ in pv_data[pv_data['eeg'] == 1].iterrows():
            self.eeg_portfolio.add_energy_system(system_.to_dict())
        for _, system_ in pv_data[pv_data['eeg'] == 0].iterrows():
            self.mrk_portfolio.add_energy_system(system_.to_dict())
        self.logger.info('Free Area PV added')

        # Construction of the pv systems (h0)
        pv_data = self.infrastructure_interface.get_solar_systems_in_area(area=plz, solar_type='roof_top')
        pv_data['type'] = 'solar'
        pv_data['open_space'] = False
        for _, system_ in pv_data[pv_data['eeg'] == 1].iterrows():
            self.eeg_portfolio.add_energy_system(system_.to_dict())
        self.logger.info('Household PV added')
        self.pv_data = pv_data

        # Construction Run River
        run_river_data = self.infrastructure_interface.get_run_river_systems_in_area(area=plz)
        run_river_data['type'] = 'water'
        for _, system_ in run_river_data.iterrows():
            self.eeg_portfolio.add_energy_system(system_.to_dict())
        self.logger.info('Run River Power Plants added')

        # Construction Biomass
        bio_mass_data = self.infrastructure_interface.get_biomass_systems_in_area(area=plz)
        bio_mass_data['type'] = 'bio'
        for _, system_ in bio_mass_data.iterrows():
            self.eeg_portfolio.add_energy_system(system_.to_dict())
        self.logger.info('Biomass Power Plants added')

        # df = pd.DataFrame(index=[pd.to_datetime(self.date)], data=self.portfolio.capacities)
        # TODO: Add Insert in TimescaleDB
        # self.connections['influxDB'].save_data(df, 'Areas', dict(typ=self.typ, agent=self.name, area=self.plz))

        self.logger.info('setup of the agent completed in %s' % (tme.time() - start_time))

    def callback(self, ch, method, properties, body):
        super().callback(ch, method, properties, body)

        message = body.decode("utf-8")
        self.date = pd.to_datetime(message.split(' ')[1])
        # Call DayAhead Optimization
        # -----------------------------------------------------------------------------------------------------------
        if 'opt_dayAhead' in message:
            try:
                self.optimize_dayAhead()
            except:
                self.logger.exception('Error during day Ahead optimization')

        # Call DayAhead Result
        # -----------------------------------------------------------------------------------------------------------
        if 'result_dayAhead' in message:
            try:
                self.post_dayAhead()
            except:
                self.logger.exception('Error in After day Ahead process')

    def optimize_dayAhead(self):
        """Scheduling before DayAhead Market"""
        self.logger.info('DayAhead market scheduling started')

        # Step 1: forecast input data and init the model for the coming day
        # -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()                                             # performance timestamp

        weather = self.weather_forecast(self.date, mean=False)              # local weather forecast dayAhead
        self.portfolio.set_parameter(self.date, weather, dict())
        self.portfolio.build_model()

        self.performance['initModel'] = np.round(tme.time() - start_time, 3)

        # Step 2: standard optimization --> returns power series in [MW]
        # -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()                                         # performance timestamp

        power_da = self.portfolio.optimize()                            # total portfolio power
        # split power in eeg and direct marketing part
        power_direct = agent.portfolio.generation['powerSolar'] + agent.portfolio.generation['powerWind']
        power_eeg = power_da - power_direct

        self.performance['optModel'] = np.round(tme.time() - start_time, 3)

        # Step 3: save optimization results in influxDB
        # -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()

        # build dataframe to save results in ifluxdb
        df = pd.concat([pd.DataFrame.from_dict(self.portfolio.generation),
                        pd.DataFrame(data=dict(powerDirect=power_direct, powerEEG=power_eeg))],
                       axis=1)
        df.index = pd.date_range(start=self.date, freq='60min', periods=len(df))
        self.connections['influxDB'].save_data(df, 'Areas', dict(typ=self.typ, agent=self.name, area=self.plz,
                                                                 timestamp='optimize_dayAhead'))

        self.performance['saveSchedule'] = np.round(tme.time() - start_time, 3)

        # Step 4: build orders from optimization results
        # -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()

        ask_orders = {}                                                     # all block orders for current day
        for i in range(self.portfolio.T):
            var_cost = 0
            ask_orders.update({str(('gen%s' % i, i, 0, str(self.name))): (var_cost, power_direct[i], 'x')})
            ask_orders.update({str(('gen%s' % (i+24), i, 0, str(self.name))): (-499, power_eeg[i], 'x')})

        self.performance['buildOrders'] = tme.time() - start_time

        # Step 5: send orders to market resp. to mongodb
        # -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()

        self.connections['mongoDB'].set_dayAhead_orders(name=self.name, date=self.date, orders=ask_orders)

        self.performance['sendOrders'] = np.round(tme.time() - start_time, 3)

        self.logger.info('DayAhead market scheduling completed')
        print('DayAhead market scheduling completed:', self.name)

    def post_dayAhead(self):
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