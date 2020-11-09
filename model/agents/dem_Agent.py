# third party modules
from sys import exit
import time as tme
import os
import argparse
import pandas as pd
import numpy as np

# model modules
os.chdir(os.path.dirname(os.path.dirname(__file__)))
from aggregation.portfolio_demand import DemandPortfolio
from agents.basic_Agent import agent as basicAgent


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--plz', type=int, required=False, default=24, help='PLZ-Agent')
    return parser.parse_args()


class DemAgent(basicAgent):

    def __init__(self, date, plz):
        super().__init__(date=date, plz=plz, typ='DEM')
        # Portfolio with the corresponding households, trade and industry
        self.logger.info('starting the agent')
        start_time = tme.time()
        self.portfolio = DemandPortfolio()

        # Construction of the prosumer with photovoltaic and battery
        for key, value in self.connections['mongoDB'].get_pv_batteries().items():
            self.portfolio.add_energy_system('PvBat' + str(key), {'PvBat' + str(key): value})
        self.logger.info('Prosumer PV-Bat added')

        # Construction consumer with photovoltaic
        for key, value in self.connections['mongoDB'].get_pvs().items():
            self.portfolio.add_energy_system('Pv' + str(key), {'Pv' + str(key): value})
        self.logger.info('Consumer PV added')

        demand = self.connections['mongoDB'].get_demand()

        # Construction Standard Consumer H0
        name = 'plz_' + str(plz) + '_h0'
        self.portfolio.add_energy_system(name, {name: {'demandP': np.round(demand['h0'] * 10 ** 6, 2), 'typ': 'H0'}})
        self.logger.info('H0 added')

        # Construction Standard Consumer G0
        name = 'plz_' + str(plz) + '_g0'
        self.portfolio.add_energy_system(name, {name: {'demandP': np.round(demand['g0'] * 10 ** 6, 2), 'typ': 'G0'}})
        self.logger.info('G0 added')

        # Construction Standard Consumer RLM
        name = 'plz_' + str(plz) + '_rlm'
        self.portfolio.add_energy_system(name, {name: {'demandP': np.round(demand['rlm'] * 10 ** 6, 2), 'typ': 'RLM'}})
        self.logger.info('RLM added')

        # If there are no power systems, terminate the agent
        if len(self.portfolio.energy_systems) == 0:
            print('Number: %s No energy systems in the area' % plz)
            exit()

        self.logger.info('setup of the agent completed in %s' % (tme.time() - start_time))

    def optimize_dayAhead(self):
        """scheduling for the DayAhead market"""
        self.logger.info('DayAhead market scheduling started')

        # Step 1: forecast input data and init the model for the coming day
        # -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()                                          # performance timestamp

        weather = self.weather_forecast(self.date, mean=False)           # local weather forecast dayAhead
        prices = self.price_forecast(self.date)                          # price forecast dayAhead
        # demand = self.demand_forecast(self.date)                       # demand forecast dayAhead
        self.portfolio.set_parameter(self.date, weather, prices)
        self.portfolio.build_model()

        self.performance['initModel'] = np.round(tme.time() - start_time,3)

        # Step 2: standard optimization --> returns power series in [kW]
        # -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()                                         # performance timestamp

        power_da = self.portfolio.optimize()                            # total portfolio power

        self.performance['optModel'] = np.round(tme.time() - start_time, 3)

        # Step 3: save optimization results in influxDB
        # -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()

        df = pd.DataFrame(data=dict(powerTotal=power_da/10**3, heatTotal=self.portfolio.demand['heat']/10**3,
                                    powerSolar=self.portfolio.generation['powerSolar']/10**3),
                          index=pd.date_range(start=self.date, freq='60min', periods=self.portfolio.T))

        self.connections['influxDB'].save_data(df, 'Areas', dict(typ=self.typ, agent=self.name, area=self.plz,
                                                                 timestamp='optimize_dayAhead'))

        self.performance['saveSchedule'] = np.round(tme.time() - start_time, 3)

        # Step 4: build orders from optimization results
        # -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()

        bid_orders = dict()
        for i in range(self.portfolio.T):
            bid_orders.update({str(('dem%s' % i, i, 0, str(self.name))): (3000, power_da[i]/10**3, 'x')})

        self.performance['buildOrders'] = np.round(tme.time() - start_time, 3)

        # Step 5: send orders to market resp. to mongodb
        # -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()

        self.connections['mongoDB'].set_dayAhead_orders(name=self.name, date=self.date, orders=bid_orders)

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

        power_da = np.asarray(self.portfolio.optimize(), np.float)                     # [kW]

        self.performance['adjustResult'] = tme.time() - start_time

        # Step 7: save adjusted results in influxdb
        # -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()

        df = pd.DataFrame(data=dict(powerTotal=power_da/10**3, heatTotal=self.portfolio.demand['heat']/10**3,
                                    powerSolar=self.portfolio.generation['powerSolar']/10**3,
                                    profit=profit))
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

        self.performance['nextDay'] = np.round(tme.time() - start_time, 3)

        df = pd.DataFrame(data=self.performance, index=[self.date])
        self.connections['influxDB'].save_data(df, 'Performance', dict(typ=self.typ, agent=self.name, area=self.plz))

        self.logger.info('Next day scheduling completed')


if __name__ == "__main__":

    args = parse_args()
    agent = DemAgent(date='2018-02-05', plz=args.plz)
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
