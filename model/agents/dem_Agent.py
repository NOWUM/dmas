# third party modules
import time as tme
import os
import argparse
import pandas as pd
import numpy as np

# model modules
os.chdir(os.path.dirname(os.path.dirname(__file__)))
from aggregation.dem_Port import demPort
from agents.basic_Agent import agent as basicAgent


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--plz', type=int, required=False, default=15, help='PLZ-Agent')
    return parser.parse_args()


class demAgent(basicAgent):

    def __init__(self, date, plz):
        super().__init__(date=date, plz=plz, exchange='Market', typ='DEM')
        # Development of the portfolio with the corresponding households, trade and industry
        self.logger.info('starting the agent')
        start_time = tme.time()
        self.portfolio = demPort(typ="DEM")

        # Construction of the prosumer with PV and battery
        for key, value in self.ConnectionMongo.getPVBatteries().items():
            self.portfolio.addToPortfolio('PvBat' + str(key), {'PvBat' + str(key): value})
        self.logger.info('Prosumer PV-Bat added')

        # Construction Consumer with PV
        for key, value in self.ConnectionMongo.getPVs().items():
            self.portfolio.addToPortfolio('Pv' + str(key), {'Pv' + str(key): value})
        self.logger.info('Consumer PV added')

        demand = self.ConnectionMongo.getDemand()

        # Construction Standard Consumer H0
        name = 'plz_' + str(plz) + '_h0'
        self.portfolio.addToPortfolio(name, {name: {'demandP': np.round(demand['h0']*10**6, 2), 'typ': 'H0'}})
        self.logger.info('H0 added')

        # Construction Standard Consumer G0
        name = 'plz_' + str(plz) + '_g0'
        self.portfolio.addToPortfolio(name, {name: {'demandP': np.round(demand['g0']*10**6, 2), 'typ': 'G0'}})
        self.logger.info('G0 added')

        # Construction Standard Consumer RLM
        name = 'plz_' + str(plz) + '_rlm'
        self.portfolio.addToPortfolio(name, {name: {'demandP': np.round(demand['rlm']*10**6, 2), 'typ': 'RLM'}})
        self.logger.info('RLM added')

        # If there are no power systems, terminate the agent
        if len(self.portfolio.energySystems) == 0:
            print('Number: %s No energy systems in the area' % plz)
            exit()

        timeDelta = tme.time() - start_time

        self.logger.info('setup of the agent completed in %s' % timeDelta)

    def optimize_dayAhead(self):
        """scheduling for the DayAhead market"""
        self.logger.info('DayAhead market scheduling started')

        # forecast and model build for the coming day
        # -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()

        self.portfolio.setPara(self.date, self.weatherForecast(self.date), self.priceForecast(self.date))
        self.portfolio.buildModel()

        self.perfLog('initModel', start_time)

        # optimzation --> returns power timeseries in [kW]
        # -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()

        power_dayAhead = np.asarray(self.portfolio.optimize(), np.float)

        # save portfolio data in influxDB
        # -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()

        time = self.date
        portfolioData = []
        for i in self.portfolio.t:
            portfolioData.append(
                {
                    "measurement": 'Areas',
                    "tags": dict(typ='DEM',                                                         # typ
                                 agent=self.name,                                                   # name
                                 area=self.plz,                                                     # area
                                 timestamp='optimize_dayAhead'),                                    # processing step
                    "time": time.isoformat() + 'Z',
                    "fields": dict(powerTotal=power_dayAhead[i]/10**3,                              # total demand power [MW]
                                   heatTotal=self.portfolio.demand['heat'][i]/10**3,                # total demand heat [MW]
                                   powerSolar=self.portfolio.generation['solar'][i]/10**3)          # total generation solar [MW]
                }
            )
            time = time + pd.DateOffset(hours=self.portfolio.dt)
        self.ConnectionInflux.saveData(portfolioData)

        self.perfLog('saveScheduling', start_time)

        # build up orderbook
        # -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()

        orderbook = dict()
        for i in range(self.portfolio.T):
            orderbook.update({'h_%s' % i: {'quantity': [power_dayAhead[i]/10**3, 0], 'price': [3000, -3000], 'hour': i, 'name': self.name}})

        self.perfLog('buildOrderbook', start_time)

        # send orderbook to market (mongoDB)
        # -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()

        self.ConnectionMongo.setDayAhead(name=self.name, date=self.date, orders=orderbook)

        self.perfLog('sendOrderbook', start_time)

        # -------------------------------------------------------------------------------------------------------------
        self.logger.info('DayAhead market scheduling completed')

    def post_dayAhead(self):
        """Scheduling after DayAhead Market"""
        self.logger.info('After DayAhead market scheduling started')

        # -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()

        # query the DayAhead results
        ask = self.ConnectionInflux.getDayAheadAsk(self.date, self.name)            # [MWh]
        bid = self.ConnectionInflux.getDayAheadBid(self.date, self.name)            # [MWh]
        price = self.ConnectionInflux.getDayAheadPrice(self.date)                   # [€/MWh]

        # calculate the profit and the new power scheduling
        profit = [float((ask[i] - bid[i]) * price[i]) for i in range(24)]           # revenue for each hour
        power_dayAhead = np.asarray(self.portfolio.optimize(), np.float)            # [kW]

        self.perfLog('optResults', start_time)

        # save energy system data in influxDB
        # -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()

        time = self.date
        portfolioData = []
        for i in self.portfolio.t:
            portfolioData.append(
                {
                    "measurement": 'Areas',
                    "tags": dict(typ='DEM',                                         # typ
                                 agent=self.name,                                   # name
                                 area=self.plz,                                     # area
                                 timestamp='post_dayAhead'),                        # processing step
                    "time": time.isoformat() + 'Z',

                    "fields": dict(powerTotal=power_dayAhead[i] / 10 ** 3,                      # total demand power [MW]
                                   heatTotal=self.portfolio.demand['heat'][i] / 10 ** 3,        # total demand heat [MW]
                                   powerSolar=self.portfolio.generation['solar'][i] / 10 ** 3,  # total generation solar [MW]
                                   profit=profit[i])
                })
            time = time + pd.DateOffset(hours=self.portfolio.dt)
        self.ConnectionInflux.saveData(portfolioData)

        self.perfLog('saveResults', start_time)

        # -------------------------------------------------------------------------------------------------------------
        self.logger.info('After DayAhead market scheduling completed')
        self.logger.info('Next day scheduling started')
        # -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()

        if self.delay <= 0:
            for key, method in self.forecasts.items():
                if key != 'weather':
                    method.collectData(self.date)
                    method.counter += 1
                    if method.counter >= method.collect:
                        method.fitFunction()
                        method.counter = 0
        else:
            self.delay -= 1

        self.perfLog('nextDay', start_time)

        # -------------------------------------------------------------------------------------------------------------
        self.logger.info('Next day scheduling completed')


if __name__ == "__main__":

    args = parse_args()
    agent = demAgent(date='2018-01-01', plz=args.plz)
    agent.ConnectionMongo.login(agent.name, False)
    try:
        agent.run_agent()
    except Exception as e:
        print(e)
    finally:
        agent.ConnectionInflux.influx.close()
        agent.ConnectionMongo.logout(agent.name)
        agent.ConnectionMongo.mongo.close()
        if not agent.connection.is_close:
            agent.connection.close()
        exit()
