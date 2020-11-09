# third party modules
from sys import exit
import time as tme
import os
import argparse
import pandas as pd
import numpy as np
import copy


# model modules
os.chdir(os.path.dirname(os.path.dirname(__file__)))
from aggregation.portfolio_powerPlant import PwpPort
from agents.basic_Agent import agent as basicAgent


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--plz', type=int, required=False, default=60, help='PLZ-Agent')
    return parser.parse_args()


class PwpAgent(basicAgent):

    def __init__(self, date, plz):
        super().__init__(date=date, plz=plz, typ='PWP')
        # Development of the portfolio with the corresponding power plants and storages

        self.logger.info('starting the agent')
        start_time = tme.time()
        self.portfolio = PwpPort(gurobi=True, T=24)

        self.init_state = {}

        self.shadow_portfolio = PwpPort(gurobi=True, T=48)

        # Construction power plants
        for key, value in self.connections['mongoDB'].get_power_plants().items():
            if value['maxPower'] > 1:
                self.portfolio.add_energy_system(key, {key: value})
                self.shadow_portfolio.add_energy_system(key, {key: value})
                self.portfolio.capacities['capacity%s' % value['fuel'].capitalize()] += value['maxPower']
        self.logger.info('Power Plants added')

        # If there are no power systems, terminate the agent
        if len(self.portfolio.energy_systems) == 0:
            print('Number: %s No energy systems in the area' % plz)
            exit()

        df = pd.DataFrame(index=[pd.to_datetime(self.date)], data=self.portfolio.capacities)
        self.connections['influxDB'].save_data(df, 'Areas', dict(typ=self.typ, agent=self.name, area=self.plz))

        # initialize dicts for optimization results
        self.portfolio_results = {key: {offset: dict(power=np.array([]),
                                                     emission=np.array([]),
                                                     fuel=np.array([]),
                                                     start=np.array([]),
                                                     obj=0)
                                        for offset in [-10, -5, 0, 5, 10]}
                                  for key, _ in self.portfolio.energy_systems.items()}

        self.shadow_results = {key: {offset: dict(power=np.array([]),
                                                  emission=np.array([]),
                                                  fuel=np.array([]),
                                                  start=np.array([]),
                                                  obj=0)
                                     for offset in [-10, -5, 0, 5, 10]}
                               for key, _ in self.portfolio.energy_systems.items()}

        self.logger.info('setup of the agent completed in %s' % (tme.time() - start_time))

    @staticmethod
    def __set_results(portfolio, result, offset, price):
        for key, value in portfolio.energy_systems.items():
            result[key][offset]['power'] = np.concatenate((result[key][offset]['power'], value['model'].power))
            result[key][offset]['emission'] = np.concatenate((result[key][offset]['emission'], value['model'].emission))
            result[key][offset]['fuel'] = np.concatenate((result[key][offset]['fuel'], value['model'].fuel))
            result[key][offset]['start'] = np.concatenate((result[key][offset]['start'], value['model'].start))
            obj = np.sum(value['model'].power * price['power']) \
                  - np.sum(value['model'].emission) - np.sum(value['model'].fuel) - np.sum(value['model'].start)
            result[key][offset]['obj'] += obj

    def optimize_dayAhead(self):
        """scheduling for the DayAhead market"""
        self.logger.info('DayAhead market scheduling started')

        # Step 1: forecast input data and init the model for the coming day
        # -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()

        weather = self.weather_forecast(self.date, mean=False, days=2)         # local weather forecast dayAhead
        # demand = self.demand_forecast(self.date)                             # demand forecast dayAhead
        prices = self.price_forecast(self.date, days=2)                        # price forecast dayAhead
        self.performance['initModel'] = self.performance['initModel'] = np.round(tme.time() - start_time, 3)

        init_state = {key: value['model'].power_plant for key, value in self.portfolio.energy_systems.items()}
        #return init_state
        self.init_state = copy.deepcopy(init_state)

        # Step 2: optimization --> returns power series in [MW]
        # -------------------------------------------------------------------------------------------------------------
        for offset in [-10, -5, 0, 5, 10]:
            for key, value in self.portfolio.energy_systems.items():
                value['model'].power_plant = copy.deepcopy(self.init_state[key])

            # prices and weather first day
            pr1 = dict(power=prices['power'][:24] + offset, gas=prices['gas'][:24], co=prices['co'][:24],
                       lignite=prices['lignite'], coal=prices['coal'], nuc=prices['nuc'])
            weather1 = dict()
            # prices and weather second day
            pr2 = dict(power=prices['power'][24:] + offset, gas=prices['gas'][24:], co=prices['co'][24:],
                       lignite=prices['lignite'], coal=prices['coal'], nuc=prices['nuc'])
            weather2 = dict()
            # prices and weather both days
            pr12 = dict(power=prices['power'] + offset, gas=prices['gas'], co=prices['co'],
                        lignite=prices['lignite'], coal=prices['coal'], nuc=prices['nuc'])
            weather12 = dict()

            self.portfolio.set_parameter(date=self.date, weather=weather1, prices=pr1)
            self.portfolio.build_model()
            power, _, _, _ = self.portfolio.optimize()

            if offset == 0:
                df = pd.DataFrame.from_dict(self.portfolio.generation)

            self.__set_results(portfolio=self.portfolio, offset=offset, result=self.portfolio_results,
                               price=pr1)

            self.portfolio.build_model(response=power)
            self.portfolio.optimize()

            self.portfolio.set_parameter(date=self.date + pd.DateOffset(days=1), weather=weather2, prices=pr2)
            self.portfolio.build_model()
            self.portfolio.optimize()

            self.__set_results(portfolio=self.portfolio, offset=offset, result=self.portfolio_results,
                               price=pr2)

            self.shadow_portfolio.set_parameter(date=self.date, weather=weather12, prices=pr12)
            self.shadow_portfolio.build_model()
            self.shadow_portfolio.optimize()

            self.__set_results(portfolio=self.shadow_portfolio, offset=offset, result=self.shadow_results,
                               price=pr12)

        self.performance['optModel'] = tme.time() - start_time

        # Step 3: save optimization results in influxDB
        # -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()

        # build dataframe to save results in ifluxdb
        df = pd.concat([df, pd.DataFrame(data=dict(frcst=pr1['power']))], axis=1)
        df.index = pd.date_range(start=self.date, freq='60min', periods=len(df))
        self.connections['influxDB'].save_data(df, 'Areas', dict(typ=self.typ, agent=self.name, area=self.plz,
                                                                 timestamp='optimize_dayAhead'))

        self.performance['saveSchedule'] = tme.time() - start_time

        # Step 4: build orders from optimization results
        # -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()
        order_book = {}

        for key, _ in self.portfolio.energy_systems.items():

            last_power = np.zeros(24)                                               # last known power
            block_number = 0                                                        # block number counter
            links = {i: 'x' for i in range(24)}                                     # current links between blocks
            name = str(self.name + '-' + key)
            prevent_starts = {}
            prevent_start_orders = {}

            d_delta = 0
            for offset in [-10, -5, 0, 5, 10]:
                power_portfolio = self.portfolio_results[key][offset]['power']
                power_shadow = self.shadow_results[key][offset]['power']
                obj_portfolio = self.portfolio_results[key][offset]['obj']
                obj_shadow = self.shadow_results[key][offset]['obj']
                delta = obj_shadow - obj_portfolio
                if power_portfolio[23] == 0:
                    hours = np.argwhere(power_portfolio[:24] == 0).reshape((-1,))
                    prevent_start = all(power_shadow[hours] > 0)
                    if prevent_start:
                        prevent_starts.update({offset: (prevent_start, obj_portfolio, obj_shadow, delta - d_delta,
                                                        hours)})
                        d_delta = delta
                    else:
                        prevent_starts.update({offset: (prevent_start, obj_portfolio, obj_shadow, delta - d_delta,
                                                        hours)})
                else:
                    prevent_starts.update({offset: (False, obj_portfolio, obj_shadow, 0,
                                                    np.argwhere(power_portfolio[:24] == 0).reshape((-1,)))})

            for offset in [-10, -5, 0, 5, 10]:
                result = self.portfolio_results[key][offset]

                # build mother order if any power > 0 for the current day and the last known power is total zero
                if np.count_nonzero(result['power'][:24]) > 0 and np.count_nonzero(last_power) == 0:
                    # calculate variable cost for each hour
                    var_cost = np.nan_to_num((result['fuel'][:24] + result['emission'][:24] + result['start'][:24]) /
                                              result['power'][:24])
                    # and get mean value for requested price
                    price = np.mean(var_cost[var_cost > 0])
                    # for each hour with power > 0 add order to order_book
                    for hour in np.argwhere(result['power'][:24] > 0).reshape((-1,)):
                        price = np.round(price, 2)
                        power = np.round(result['power'][hour])
                        order_book.update({str(('gen0', hour, 0, name)): (price, power, 0)})
                        links.update({hour: block_number})
                    block_number += 1                       # increment block number
                    last_power = result['power'][:24]       # set last_power to current power

                if prevent_starts[offset][0]:
                    result = self.shadow_results[key][offset]
                    hours = prevent_starts[offset][4]
                    factor = prevent_starts[offset][3] / np.sum(result['power'][hours])
                    # for each hour with power > 0 add order to order_book
                    link = 0
                    if len(prevent_start_orders) == 0:
                        for hour in hours:
                            price = (result['fuel'][hour] + result['emission'][hour]) / result['power'][hour]
                            price = np.round(price - factor, 2)
                            power = np.round(result['power'][hour])
                            prevent_start_orders.update(
                                {str(('gen%s' % block_number, hour, 0, name)): (price, power, link)})
                            order_book.update({str(('gen%s' % block_number, hour, 0, name)): (price, power, link)})
                            link = block_number
                            links.update({hour: block_number})
                            block_number += 1  # increment block number
                    else:
                        for hour in hours:
                            for id_, order in prevent_start_orders.items():
                                if id_[1] == hour:
                                    order = {id_: (np.round(order[0] - factor, 2),
                                                   np.round(result['power'][hour], 2),
                                                   order[2])}
                                    prevent_start_orders.update(order)
                                    order_book.update(order)

                    last_power = result['power'][:24]

                # add linked hour blocks
                # check if current power is higher then the last known power
                if np.count_nonzero(result['power'][:24] - last_power) > 0:
                    delta = result['power'][:24] - last_power  # get deltas
                    stack_vertical = np.argwhere(last_power > 0).reshape((-1,))  # and check if last_power > 0
                    # for each power with last_power > 0
                    for hour in stack_vertical:
                        # check if delta > 0
                        if delta[hour] > 0:
                            # calculate variable cost for the hour and set it as requested price
                            price = np.round((result['fuel'][hour] + result['emission'][hour]) / result['power'][hour] / result['power'][hour], 2)
                            power = np.round(0.2 * delta[hour], 2)
                            # check if the last linked block for this hour is unknown
                            if links[hour] == 'x':
                                link = 0  # if unknown, link to mother order
                            else:
                                link = links[hour]  # else link to last block for this hour
                            # split volume in five orders and add them to order_book
                            for order in range(5):
                                order_book.update({str(('gen%s' % block_number, hour, order, name)): (price,
                                                                                                      power,
                                                                                                      link)})
                            links.update({hour: block_number})  # update last known block for hour
                            block_number += 1  # increment block number

                    left = stack_vertical[0]  # get first left hour from last_power   ->  __|-----|__
                    right = stack_vertical[-1]  # get first right hour from last_power  __|-----|__ <--

                    # if the left hour differs from first hour of the current day
                    if left > 0:
                        # build array for e.g. [0,1,2,3,4,5, ..., left]
                        stack_left = np.arange(start=left - 1, stop=-1, step=-1)
                        # check if the last linked block for the fist left hour is unknown
                        # (only first hour is connected to mother)
                        if links[stack_left[0]] == 'x':
                            link = 0  # if unknown, link to mother order
                        else:
                            link = links[stack_left[0]]  # else link to last block for this hour
                        # for each hour in left_stack
                        for hour in stack_left:
                            # check if delta > 0
                            if delta[hour] > 0:
                                # calculate variable cost for the hour and set it as requested price
                                price = np.round((result['fuel'][hour] + result['emission'][hour]) / result['power'][hour] / result['power'][hour], 2)
                                power = np.round(0.2 * delta[hour], 2)
                                # split volume in five orders and add them to order_book
                                for order in range(5):
                                    order_book.update({str(('gen%s' % block_number, hour, order, name)): (price,
                                                                                                          power,
                                                                                                          link)})
                                link = block_number
                                links.update({hour: block_number})  # update last known block for hour
                                block_number += 1  # increment block number

                    # if the right hour differs from last hour of the current day
                    if right < 23:
                        # build array for e.g. [right, ... ,19,20,21,22,23]
                        stack_right = np.arange(start=right + 1, stop=24)
                        # check if the last linked block for the fist right hour is unknown
                        # (only first hour is connected to mother)
                        if links[stack_right[0]] == 'x':
                            link = 0  # if unknown, link to mother order
                        else:
                            link = links[stack_right[0]]  # else link to last block for this hour
                        for hour in stack_right:
                            # check if delta > 0
                            if delta[hour] > 0:
                                # calculate variable cost for the hour and set it as requested price
                                price = np.round((result['fuel'][hour] + result['emission'][hour]) / result['power'][hour] / result['power'][hour], 2)
                                power = np.round(0.2 * delta[hour], 2)
                                # split volume in five orders and add them to order_boo
                                for order in range(5):
                                    order_book.update({str(('gen%s' % block_number, hour, order, name)): (price,
                                                                                                          power,
                                                                                                          link)})
                                link = block_number
                                links.update({hour: block_number})  # update last known block for hour
                                block_number += 1  # increment block number

                    last_power = result['power'][:24]  # set last_power to current power

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
        for key, value in self.portfolio.energy_systems.items():
            value['model'].power_plant = copy.deepcopy(self.init_state[key])
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
