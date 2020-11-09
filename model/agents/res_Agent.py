# third party modules
from sys import exit
import time as tme
import os
import argparse
import pandas as pd
import numpy as np

# model modules
os.chdir(os.path.dirname(os.path.dirname(__file__)))
from aggregation.portfolio_renewable import RenewablePortfolio
from agents.basic_Agent import agent as basicAgent


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--plz', type=int, required=False, default=12, help='PLZ-Agent')
    return parser.parse_args()


class ResAgent(basicAgent):

    def __init__(self, date, plz):
        super().__init__(date=date, plz=plz, typ='RES')
        # Development of the portfolio with the corresponding ee-systems
        self.logger.info('starting the agent')
        start_time = tme.time()
        self.portfolio = RenewablePortfolio()

        # Construction Windenergy
        for key, value in self.connections['mongoDB'].get_wind_turbines().items():
            self.portfolio.capacities['capacityWind'] += value['maxPower']
            self.portfolio.add_energy_system(key, {key: value})
        self.logger.info('Windenergy added')
        self.portfolio.aggregate_wind()

        # Construction of the pv systems (free area)
        for key, value in self.connections['mongoDB'].get_pv_parks().items():
            if value['typ'] != 'PV70':
                self.portfolio.capacities['capacitySolar'] += value['maxPower']/10**3
            else:
                self.portfolio.capacities['capacitySolar'] += value['maxPower']/10**3 * value['number']
            self.portfolio.add_energy_system(key, {key: value})
        self.logger.info('PV(free area) Generation added')

        # Construction of the pv systems (h0)
        for key, value in self.connections['mongoDB'].get_pvs().items():
            self.portfolio.capacities['capacitySolar'] += value['PV']['maxPower']/10**3 * value['EEG']
            self.portfolio.add_energy_system('Pv' + str(key), {'Pv' + str(key): value})
        self.logger.info('PV(H0) Generation added')

        # Construction Run River
        for key, value in self.connections['mongoDB'].get_runriver_systems().items():
            self.portfolio.add_energy_system('runRiver', {'runRiver': value})
            self.portfolio.capacities['capacityWater'] += value['maxPower']/10**3
        self.logger.info('Run River Power Plants added')

        # Construction Biomass
        for key, value in self.connections['mongoDB'].get_biomass_systems().items():
            self.portfolio.add_energy_system('bioMass', {'bioMass': value})
            self.portfolio.capacities['capacityBio'] += value['maxPower']/10**3
        self.logger.info('Biomass Power Plants added')

        # If there are no power systems, terminate the agent
        if len(self.portfolio.energy_systems) == 0:
            print('Number: %s No energy systems in the area' % plz)
            exit()

        df = pd.DataFrame(index=[pd.to_datetime(self.date)], data=self.portfolio.capacities)
        self.connections['influxDB'].save_data(df, 'Areas', dict(typ=self.typ, agent=self.name, area=self.plz))

        self.logger.info('setup of the agent completed in %s' % (tme.time() - start_time))

    def optimize_dayAhead(self):
        """Scheduling before DayAhead Market"""
        self.logger.info('DayAhead market scheduling started')

        # Step 1: forecast input data and init the model for the coming day
        # -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()                                         # performance timestamp

        weather = self.weather_forecast(self.date, mean=False)           # local weather forecast dayAhead
        prices = self.price_forecast(self.date)                          # price forecast dayAhead
        demand = self.demand_forecast(self.date)                         # demand forecast dayAhead
        self.portfolio.set_parameter(self.date, weather, prices)
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
                        pd.DataFrame(data=dict(powerDirect=power_direct, powerEEG=power_eeg, frcst=prices['power']))],
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
        self.logger.info('Next day scheduling started')

        # Step 8: retrain forecast methods and learning algorithm
        # -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()

        # collect data an retrain forecast method
        dem = self.connections['influxDB'].get_dem(self.date)                               # demand germany [MW]
        weather = self.connections['influxDB'].get_weather(self.geo, self.date, mean=True)  # mean weather germany
        prc_1 = self.connections['influxDB'].get_prc_da(self.date-pd.DateOffset(days=1))    # mcp yesterday [€/MWh]
        prc_7 = self.connections['influxDB'].get_prc_da(self.date-pd.DateOffset(days=7))    # mcp week before [€/MWh]
        for key, method in self.forecasts.items():
            method.collect_data(date=self.date, dem=dem, prc=prc, prc_1=prc_1, prc_7=prc_7, weather=weather)
            method.counter += 1
            if method.counter >= method.collect:                                            # retrain forecast method
                method.fit_function()
                method.counter = 0

        df = pd.DataFrame(index=[pd.to_datetime(self.date)], data=self.portfolio.capacities)
        self.connections['influxDB'].save_data(df, 'Areas', dict(typ=self.typ, agent=self.name, area=self.plz))

        self.performance['nextDay'] = np.round(tme.time() - start_time, 3)

        df = pd.DataFrame(data=self.performance, index=[self.date])
        self.connections['influxDB'].save_data(df, 'Performance', dict(typ=self.typ, agent=self.name, area=self.plz))

        self.logger.info('Next day scheduling completed')


if __name__ == "__main__":

    args = parse_args()
    agent = ResAgent(date='2018-01-01', plz=args.plz)
    agent.connections['mongoDB'].login(agent.name)
    try:
        agent.run()
    except Exception as e:
        print(e)
    finally:
        agent.connections['mongoDB'].logout(agent.name)
        agent.connections['influxDB'].influx.close()
        agent.connections['mongoDB'].mongo.close()
        if not agent.connections['connectionMQTT'].is_closed:
            agent.connections['connectionMQTT'].close()
        exit()
