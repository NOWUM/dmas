# third party modules
import time as tme
import os
import argparse
import pandas as pd
import numpy as np
from scipy.stats import norm

# model modules
os.chdir(os.path.dirname(os.path.dirname(__file__)))
from aggregation.portfolio_storage import StrPort
from agents.basic_Agent import agent as basicAgent


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--plz', type=int, required=False, default=57, help='PLZ-Agent')
    return parser.parse_args()


class StrAgent(basicAgent):

    def __init__(self, date, plz):
        super().__init__(date=date, plz=plz, typ='STR')
        # Development of the portfolio with the corresponding power plants and storages
        self.logger.info('starting the agent')
        start_time = tme.time()
        self.portfolio = StrPort(gurobi=True, T=24)

        # Construction storages
        for key, value in self.connections['mongoDB'].get_storages().items():
            self.portfolio.capacities['capacityWater'] += value['P+_Max']
            self.portfolio.add_energy_system(key, {key: value})
        self.logger.info('Storages added')

        mcp = [37.70, 35.30, 33.90, 33.01, 33.27, 35.78, 43.17, 50.21, 52.89, 51.18, 48.24, 46.72, 44.23,
               42.29, 41.60, 43.12, 45.37, 50.95, 55.12, 56.34, 52.70, 48.20, 45.69, 40.25]
        self.base_price = np.asarray(mcp).reshape((1,-1))

        if len(self.forecasts['price'].y) > 0:
            self.base_price = self.forecasts['price'].y

        self.q_ask = 0
        self.q_bid = 0
        # If there are no power systems, terminate the agent
        if len(self.portfolio.energy_systems) == 0:
            print('Number: %s No energy systems in the area' % plz)
            exit()

        df = pd.DataFrame(index=[pd.to_datetime(self.date)], data=self.portfolio.capacities)
        self.connections['influxDB'].save_data(df, 'Areas', dict(typ=self.typ, agent=self.name, area=self.plz))

        self.logger.info('setup of the agent completed in %s' % (tme.time() - start_time))

    def optimize_dayAhead(self):
        """scheduling for the DayAhead market"""
        self.logger.info('DayAhead market scheduling started')

        # Step 1: forecast input data and init the model for the coming day
        # -------------------------------------------------------------------------------------------------------------

        prices = self.price_forecast(self.date)                        # price forecast dayAhead
        self.portfolio.set_parameter(self.date, dict(), prices)
        self.portfolio.build_model()
        self.portfolio.optimize()

        base_prc = np.mean(prices['power'])
        std_prc = np.sqrt(np.var(np.mean(self.base_price, axis=1)))

        order_book = {}

        for key, value in self.portfolio.energy_systems.items():
            eta = value['eta+'] * value['eta-']
            min_ask_prc = base_prc * (1.5 - eta/2)
            max_bid_prc = base_prc * (0.5 + eta/2)

            vol_ask = [0.15, 0.25, 0.25, 0.15, 0.10, 0.05, 0.05]
            prc_ask = ['x', 0.55, 0.60, 0.66, 0.75, 0.85, 0.95]

            vol_bid = [0.15, 0.25, 0.25, 0.15, 0.1, 0.05, 0.05]
            prc_bid = ['x', 0.40, 0.36, 0.31, 0.25, 0.15, 0.5]

            block_bid_number = 0
            block_ask_number = 0

            for i in self.portfolio.t:
                for k in range(len(prc_bid)):
                    if prc_bid[k] != 'x':
                        prc = np.round(min(norm.pdf(prc_bid[k]+self.q_bid)*std_prc+base_prc, max_bid_prc), 2)
                    else:
                        prc = max_bid_prc

                    vol = np.round(vol_bid[k] * value['P+_Max'], 2)
                    order_book.update({str(('dem%s' % block_bid_number, i, k, self.name)): (prc, vol, 'x')})

                block_bid_number += 1

                for k in range(len(prc_ask)):
                    if prc_bid[k] != 'x':
                        prc = np.round(max(norm.pdf(prc_ask[k]+self.q_ask)*std_prc+base_prc, min_ask_prc), 2)
                    else:
                        prc = min_ask_prc

                    vol = np.round(vol_ask[k] * value['P+_Max'], 2)
                    order_book.update({str(('gen%s' % block_ask_number, i, k, self.name)): (prc, vol, 'x')})

                block_ask_number += 1

        # Step 5: send orders to market resp. to mongodb
        # -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()

        self.connections['mongoDB'].set_dayAhead_orders(name=self.name, date=self.date, orders=order_book)

        self.performance['sendOrders'] = tme.time() - start_time

        self.logger.info('DayAhead market scheduling completed')

    def post_dayAhead(self):
        """Scheduling after DayAhead Market"""
        self.logger.info('After DayAhead market scheduling started')

        # Step 6: get market results and adjust generation
        # -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()

        # query the DayAhead results
        ask = self.connections['influxDB'].get_ask_da(self.date, self.name)            # volume to buy
        bid = self.connections['influxDB'].get_bid_da(self.date, self.name)            # volume to sell
        prc = self.connections['influxDB'].get_prc_da(self.date)                       # market clearing price
        profit = (ask - bid) * prc

        self.week_price_list.remember_price(prcToday=prc)

        v0 = self.portfolio.volume[0]
        # case 1
        # --> q_bid ++
        # --> q_ask --
        if (v0 + sum(bid) * 0.9 - sum(ask)/0.9) > 0:
            self.q_ask -= 0.005
            self.q_bid += 0.005
        # case 2
        # --> q_bid --
        # --> q_ask ++
        elif (v0 + sum(bid) * 0.9 - sum(ask)/0.9) < 0:
            self.q_ask += 0.005
            self.q_bid -= 0.005

        self.q_ask = max(min(0.04, self.q_ask), -0.04)
        self.q_bid = max(min(0.04, self.q_bid), -0.04)

        # adjust power generation
        self.portfolio.build_model(response=ask - bid)
        self.portfolio.optimize()
        volume = self.portfolio.volume
        self.performance['adjustResult'] = tme.time() - start_time

        self.base_price = np.concatenate((self.base_price, prc.reshape((1, 24))), axis=0)

        # Step 7: save adjusted results in influxdb
        # -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()

        df = pd.concat([pd.DataFrame.from_dict(self.portfolio.generation),
                        pd.DataFrame(data=dict(profit=profit, volume=volume))], axis=1)
        df.index = pd.date_range(start=self.date, freq='60min', periods=len(df))
        self.connections['influxDB'].save_data(df, 'Areas', dict(typ=self.typ, agent=self.name, area=self.plz,
                                                                 timestamp='post_dayAhead'))

        self.performance['saveResult'] = tme.time() - start_time

        self.logger.info('After DayAhead market adjustment completed')
        self.logger.info('Next day scheduling started')

        # Step 8: retrain forecast methods and learning algorithm
        # -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()

        # collect data an retrain forecast method
        dem = self.connections['influxDB'].get_dem(self.date)  # demand germany [MW]
        weather = self.forecasts['weather'].mean_weather  # weather data
        prc_1 = self.week_price_list.get_price_yesterday()  # mcp yesterday [€/MWh]
        prc_7 = self.week_price_list.get_pirce_week_before()  # mcp week before [€/MWh]
        for key, method in self.forecasts.items():
            method.collect_data(date=self.date, dem=dem, prc=prc[:24], prc_1=prc_1, prc_7=prc_7, weather=weather)
            method.counter += 1
            if method.counter >= method.collect:  # retrain forecast method
                method.fit_function()
                method.counter = 0

        self.week_price_list.put_price()

        df = pd.DataFrame(index=[pd.to_datetime(self.date)], data=self.portfolio.capacities)
        self.connections['influxDB'].save_data(df, 'Areas', dict(typ=self.typ, agent=self.name, area=self.plz))

        self.performance['nextDay'] = tme.time() - start_time

        #df = pd.DataFrame(data=self.performance, index=[self.date])
        #self.connections['influxDB'].save_data(df, 'Performance', dict(typ=self.typ, agent=self.name, area=self.plz))

        self.logger.info('Next day scheduling completed')


if __name__ == "__main__":

    args = parse_args()
    agent = StrAgent(date='2018-01-01', plz=args.plz)
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
