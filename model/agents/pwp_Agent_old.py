# third party modules
from sys import exit
import time as tme
import os
import argparse
import pandas as pd
import numpy as np

# model modules
os.chdir(os.path.dirname(os.path.dirname(__file__)))
from aggregation.portfolio_powerPlant import PwpPort
from agents.basic_Agent import agent as basicAgent


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--plz', type=int, required=False, default=50, help='PLZ-Agent')
    return parser.parse_args()


class PwpAgent(basicAgent):

    def __init__(self, date, plz):
        super().__init__(date=date, plz=plz, typ='PWP')
        # Development of the portfolio with the corresponding power plants and storages
        self.logger.info('starting the agent')
        start_time = tme.time()
        self.portfolio = PwpPort(gurobi=True, T=24)

        # Construction power plants
        for key, value in self.connections['mongoDB'].get_power_plants().items():
            if value['maxPower'] > 1:
                self.portfolio.add_energy_system(key, {key: value})
                self.portfolio.capacities['capacity%s' % value['fuel'].capitalize()] += value['maxPower']
        self.logger.info('Power Plants added')

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
        start_time = tme.time()

        weather = self.weather_forecast(self.date, mean=False)         # local weather forecast dayAhead
        # demand = self.demand_forecast(self.date)                     # demand forecast dayAhead
        prices = self.price_forecast(self.date)                        # price forecast dayAhead
        dayAhead_prc = prices['power']
        self.performance['initModel'] = self.performance['initModel'] = np.round(tme.time() - start_time, 3)

        # Step 2: optimization --> returns power series in [MW]
        # -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()

        offsets = [-7, -5, 0, 5, 7, 'max']
        results = {offset: {} for offset in offsets}
        for offset in offsets:
            if offset == 'max':
                prices.update({'power': dayAhead_prc})
                self.portfolio.set_parameter(self.date, weather, prices)
                self.portfolio.build_model(max_power=True)
            else:
                prices.update({'power': dayAhead_prc + offset})
                self.portfolio.set_parameter(self.date, weather, prices)
                self.portfolio.build_model()
            self.portfolio.optimize()
            for key, value in self.portfolio.energy_systems.items():
                results[offset].update({key: (value['model'].power,
                                              value['model'].emission,
                                              value['model'].fuel,
                                              value['model'].start)})
            if offset == 0:
                df = pd.DataFrame.from_dict(self.portfolio.generation)

        self.performance['optModel'] = self.performance['initModel'] = np.round(tme.time() - start_time, 3)

        # Step 3: save optimization results in influxDB
        # -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()

        # build dataframe to save results in ifluxdb
        df = pd.concat([df, pd.DataFrame(data=dict(frcst=dayAhead_prc))], axis=1)
        df.index = pd.date_range(start=self.date, freq='60min', periods=len(df))
        self.connections['influxDB'].save_data(df, 'Areas', dict(typ=self.typ, agent=self.name, area=self.plz,
                                                                 timestamp='optimize_dayAhead'))

        self.performance['saveSchedule'] = self.performance['initModel'] = np.round(tme.time() - start_time, 3)

        # Step 4: build orders from optimization results
        # -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()

        order_book = {}
        for key, _ in self.portfolio.energy_systems.items():

            last_power = np.zeros(24)
            block_number = 0
            links = {i: 'x' for i in range(24)}
            name = str(self.name + '-' + key)
            for _, value in results.items():
                value = value[key]
                # build mother order
                if np.count_nonzero(value[0]) > 0 and np.count_nonzero(last_power) == 0:
                    var_cost = np.nan_to_num((value[1] + value[2] + value[3]) / value[0])
                    price = np.mean(var_cost[var_cost > 0])
                    for hour in np.argwhere(value[0] > 0).reshape((-1,)):
                        price = np.round(price, 2)
                        power = np.round(value[0][hour])
                        order_book.update({str(('gen0', hour, 0, name)): (price, power, 0)})
                        links.update({hour: block_number})
                    block_number += 1
                    last_power = value[0]

                # add linked hour blocks
                elif np.count_nonzero(value[0] - last_power) > 0:
                    delta = value[0] - last_power
                    stack_vertical = np.argwhere(last_power > 0).reshape((-1,))
                    for hour in stack_vertical:
                        if delta[hour] > 0:
                            price = np.round((value[1][hour] + value[2][hour]) / value[0][hour], 2)
                            power = np.round(0.2 * delta[hour])
                            if links[hour] == 'x':
                                link = 0
                            else:
                                link = links[hour]
                            for order in range(5):
                                order_book.update({str(('gen%s' % block_number, hour, order, name)): (price,
                                                                                                      power,
                                                                                                      link)})
                            links.update({hour: block_number})
                            block_number += 1

                    left = stack_vertical[0]
                    right = stack_vertical[-1]

                    if left > 0:
                        stack_left = np.arange(start=left-1, stop=-1, step=-1)
                        if links[stack_left[0]] == 'x':
                            link = 0
                        else:
                            link = links[stack_left[0]]
                        for hour in stack_left:
                            if delta[hour] > 0:
                                price = np.round((value[1][hour] + value[2][hour]) / value[0][hour], 2)
                                power = np.round(0.2 * delta[hour])
                                for order in range(5):
                                    order_book.update({str(('gen%s' % block_number, hour, order, name)): (price,
                                                                                                          power,
                                                                                                          link)})
                                link = block_number
                                links.update({hour: block_number})
                                block_number += 1

                    elif right < 23:
                        stack_right = np.arange(start=right + 1, stop=24)
                        if links[stack_right[0]] == 'x':
                            link = 0
                        else:
                            link = links[stack_right[0]]
                        for hour in stack_right:
                            if delta[hour] > 0:
                                price = np.round((value[1][hour] + value[2][hour]) / value[0][hour], 2)
                                power = np.round(0.2 * delta[hour])
                                for order in range(5):
                                    order_book.update({str(('gen%s' % block_number, hour, order, name)): (price,
                                                                                                          power,
                                                                                                          link)})
                                link = block_number
                                links.update({hour: block_number})
                                block_number += 1

                    last_power = value[0]
        # return results, order_book
        self.performance['buildOrders'] = self.performance['initModel'] = np.round(tme.time() - start_time, 3)

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

        # adjust power generation
        self.portfolio.build_model(response=ask - bid)
        power_da, emission, fuel, _ = self.portfolio.optimize()
        self.performance['adjustResult'] = self.performance['initModel'] = np.round(tme.time() - start_time, 3)

        # Step 7: save adjusted results in influxdb
        # -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()

        df = pd.concat([pd.DataFrame.from_dict(self.portfolio.generation),
                        pd.DataFrame(data=dict(profit=profit, emissionAdjust=emission, fuelAdjust=fuel))], axis=1)
        df.index = pd.date_range(start=self.date, freq='60min', periods=len(df))
        self.connections['influxDB'].save_data(df, 'Areas', dict(typ=self.typ, agent=self.name, area=self.plz,
                                                                 timestamp='post_dayAhead'))

        self.performance['saveResult'] = self.performance['initModel'] = np.round(tme.time() - start_time, 3)

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
            method.collect_data(date=self.date, dem=dem, prc=prc[:24], prc_1=prc_1, prc_7=prc_7, weather=weather)
            method.counter += 1
            if method.counter >= method.collect:                                            # retrain forecast method
                method.fit_function()
                method.counter = 0

        df = pd.DataFrame(index=[pd.to_datetime(self.date)], data=self.portfolio.capacities)
        self.connections['influxDB'].save_data(df, 'Areas', dict(typ=self.typ, agent=self.name, area=self.plz))

        self.performance['nextDay'] = self.performance['initModel'] = np.round(tme.time() - start_time, 3)

        df = pd.DataFrame(data=self.performance, index=[self.date])
        self.connections['influxDB'].save_data(df, 'Performance', dict(typ=self.typ, agent=self.name, area=self.plz))

        self.logger.info('Next day scheduling completed')


if __name__ == "__main__":

    # from matplotlib import pyplot as plt

    args = parse_args()
    agent = PwpAgent(date='2018-01-01', plz=args.plz)
    # agent.connections['mongoDB'].login(agent.name, False)
    # try:
    #     agent.run()
    # except Exception as e:
    #     print(e)
    # finally:
    #     agent.connections['mongoDB'].logout(agent.name)
    #     agent.connections['influxDB'].influx.close()
    #     agent.connections['mongoDB'].mongo.close()
    #     if not agent.connections['connectionMQTT'].is_closed:
    #         agent.connections['connectionMQTT'].close()
    #     exit()
