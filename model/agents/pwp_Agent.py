# third party modules
import time as tme
import os
import argparse
import pandas as pd
import numpy as np
from math import radians

# model modules
os.chdir(os.path.dirname(os.path.dirname(__file__)))
from aggregation.pwp_Port import PwpPort
from agents.basic_Agent import agent as basicAgent


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--plz', type=int, required=False, default=57, help='PLZ-Agent')
    return parser.parse_args()


class PwpAgent(basicAgent):

    def __init__(self, date, plz):
        super().__init__(date=date, plz=plz, exchange='Market', typ='PWP')
        # Development of the portfolio with the corresponding power plants and storages
        self.logger.info('starting the agent')
        start_time = tme.time()
        self.portfolio = PwpPort(gurobi=True, T=48)

        # Construction power plants
        for key, value in self.connections['mongoDB'].getPowerPlants().items():
            if value['maxPower'] > 1:
                self.portfolio.add_energy_system(key, {key: value})
                self.portfolio.capacities['capacity%s' % value['fuel'].capitalize()] += value['maxPower']
        self.logger.info('Power Plants added')

        # Construction storages
        for key, value in self.connections['mongoDB'].getStorages().items():
            self.portfolio.capacities['capacityWater'] += value['P+_Max']
            self.portfolio.add_energy_system(key, {key: value})
        self.logger.info('Storages added')

        # If there are no power systems, terminate the agent
        if len(self.portfolio.energySystems) == 0:
            print('Number: %s No energy systems in the area' % plz)
            exit()

        self.strategy['qLearn'].qus *= 0.5 * (self.portfolio.capacities['capacityGas'] +
                                              self.portfolio.capacities['capacityLignite'] +
                                              self.portfolio.capacities['capacityCoal'] +
                                              self.portfolio.capacities['capacityNuc'])

        df = pd.DataFrame(index=[pd.to_datetime(self.date)], data=self.portfolio.capacities)
        self.connections['influxDB'].save_data(df, 'Areas', dict(typ=self.typ, agent=self.name, area=self.plz))

        self.logger.info('setup of the agent completed in %s' % (tme.time() - start_time))

    def optimize_dayAhead(self):
        """scheduling for the DayAhead market"""
        self.logger.info('DayAhead market scheduling started')

        # Step 1: forecast input data and init the model for the coming day
        # -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()

        weather = self.weather_forecast(self.date, 2, mean=False)           # local weather forecast dayAhead
        prices = self.price_forecast(self.date, 2)                          # price forecast dayAhead
        demand = self.demand_forecast(self.date, 2)                         # demand forecast dayAhead
        self.portfolio.set_parameter(self.date, weather, prices)
        self.portfolio.build_model()

        self.performance['initModel'] = tme.time() - start_time

        # Step 2: standard optimization --> returns power series in [MW]
        # -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()

        power_da, emission_opt, fuel_opt = self.portfolio.optimize()
        df = pd.DataFrame.from_dict(self.portfolio.generation)
        power_water = df['powerWater'].to_numpy()
        volume = self.portfolio.volume
        power_da -= power_water

        self.portfolio.build_model(max_=True)
        power_max, emission_max, fuel_max = self.portfolio.optimize()

        self.performance['optModel'] = tme.time() - start_time

        # Step 3: save optimization results in influxDB
        # -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()

        # build dataframe to save results in ifluxdb
        df = pd.concat([df, pd.DataFrame(data=dict(powerMax=power_max, emissionMax=emission_max, fuelMax=fuel_max,
                                                   emissionOpt=emission_opt, fuelOpt=fuel_opt, frcst=prices['power'],
                                                   volume=volume))], axis=1)
        df.index = pd.date_range(start=self.date, freq='60min', periods=len(df))
        df['powerTotal'] = power_da

        self.connections['influxDB'].save_data(df, 'Areas', dict(typ=self.typ, agent=self.name, area=self.plz,
                                                                 timestamp='optimize_dayAhead'))

        self.performance['saveSchedule'] = tme.time() - start_time

        # Step 4: build orders from optimization results
        # -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()

        order_book = dict()

        self.strategy['minPrice'] = np.zeros_like(power_da)             # set minimal price in [€/MWh]
        on_hour = power_da > 0                                          # generation hours
        off_hour = power_da == 0                                        # non-running hours

        self.strategy['minPrice'][on_hour] = (emission_opt[on_hour] + fuel_opt[on_hour])/power_da[on_hour]
        self.strategy['minPrice'][off_hour] = 0

        self.strategy['maxPrice'] = np.zeros_like(power_da)              # set maximal price in [€/MWh]
        on_hour = power_max > 0                                          # generation hours
        off_hour = power_max == 0                                        # non-running hours

        self.strategy['maxPrice'][on_hour] = (emission_max[on_hour] + fuel_max[on_hour]) / power_max[on_hour]
        self.strategy['maxPrice'][off_hour] = 0

        # initialize learning algorithm
        self.strategy['qLearn'].collect_data(dem=demand, prc=prices['power'], weather=weather)
        # find best action according to the actual situation
        if self.strategy['qLearn'].fitted:              # if a model is available/trained find best actions
            opt_actions = self.strategy['qLearn'].get_actions()
            actions = [opt_actions[i] if self.strategy['epsilon'] < np.random.uniform(0, 1) else
                       np.random.randint(1, 8) for i in range(24)]
        else:                                           # else try random actions to get new hints
            actions = np.random.randint(1, 8, 24) * 10

        self.strategy['actions'] = np.asarray(actions).reshape(-1, )

        for i in range(int(self.portfolio.T/2)):

            # power plant strategy
            if power_da[i] > 0:                                     # check if portfolio generate power
                # calculate slope for linear order function
                delta = (prices['power'][i] - self.strategy['minPrice'][i]) / 100
                slope = np.max(delta, 0) * np.tan(radians(self.strategy['actions'][i] + 10))
                # set price limits
                lb = self.strategy['minPrice'][i]                   # get at least variable costs (emission + fuel)
                ub = prices['power'][i]                             # get maximal the expected market clearing
                # get price volume combinations
                price = [float(min(slope * p + lb, ub)) for p in range(2, 102, 2)]
                volume = [(-2 / 100 * power_da[i]) for _ in range(2, 102, 2)]
                if power_max[i] > power_da[i]:                      # check if portfolio not generate maximal power
                    price.append(self.strategy['maxPrice'][i])      # add maximal variable costs
                    volume.append(-1*(power_max[i] - power_da[i]))  # add delta between scheduled and maximal power
                # add order for the corresponding hour
                order_book.update({'h_%s' % i: {'quantity': volume, 'price': price, 'hour': i, 'name': self.name}})
            else:
                if power_max[i] > 0:
                    price = (emission_max[i] + fuel_max[i])/power_max[i]
                    order_book.update({'h_%s' % i: {'quantity': [-power_max[i]], 'price': [price], 'hour': i,
                                                    'name': self.name}})
                else:
                    order_book.update({'h_%s' % i: {'quantity': [0], 'price': [0], 'hour': i, 'name': self.name}})

            # storage strategy
            if power_water[i] < 0:
                # calculate slope for linear order function
                delta = (prices['power'][i] - 0) / 100
                slope = np.abs(delta) * np.tan(radians(45))
                if delta > 0:
                    lb = 0                                          # pay at least 0
                    ub = prices['power'][i]                         # pay maximal the expected market clearing
                    price = [float(min(slope * p + lb, ub)) for p in range(2, 102, 2)]
                else:
                    lb = prices['power'][i]                         # pay at least the expected market clearing
                    ub = 0                                          # pay maximal 0
                    price = [float(min(slope * p + lb, ub)) for p in range(2, 102, 2)]
                volume = [(-2 / 100 * power_water[i]) for _ in range(2, 102, 2)]
                # add order for the corresponding hour
                order_book.update({'h_%s' % i: {'quantity': volume, 'price': price, 'hour': i, 'name': self.name}})
            if power_water[i] > 0:                                     # check if portfolio generate power
                # calculate slope for linear order function
                delta = (prices['power'][i] - 0) / 100
                slope = np.max(delta, 0) * np.tan(radians(45))
                if delta > 0:
                    lb = 0                                          # get at least 0
                    ub = prices['power'][i]                         # get maximal the expected market clearing
                    price = [float(min(slope * p + lb, ub)) for p in range(2, 102, 2)]
                else:
                    lb = prices['power'][i]                         # get at least the expected market clearing
                    ub = 0                                          # pay maximal 0
                    price = [float(min(slope * p + lb, ub)) for p in range(2, 102, 2)]
                volume = [(-2 / 100 * power_water[i]) for _ in range(2, 102, 2)]
                # add order for the corresponding hour
                order_book.update({'h_%s' % i: {'quantity': volume, 'price': price, 'hour': i, 'name': self.name}})

        self.performance['buildOrders'] = tme.time() - start_time

        # Step 5: send orders to market resp. to mongodb
        # -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()

        self.connections['mongoDB'].setDayAhead(name=self.name, date=self.date, orders=order_book)

        self.performance['sendOrders'] = tme.time() - start_time

        self.logger.info('DayAhead market scheduling completed')

    def post_dayAhead(self):
        """Scheduling after DayAhead Market"""
        self.logger.info('After DayAhead market scheduling started')

        # Step 6: get market results and adjust generation an strategy
        # -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()

        # query the DayAhead results
        ask = self.connections['influxDB'].get_ask_da(self.date, self.name, days=2)            # volume to buy
        bid = self.connections['influxDB'].get_bid_da(self.date, self.name, days=2)            # volume to sell
        prc = self.connections['influxDB'].get_prc_da(self.date, days=2)                       # market clearing price
        profit = (ask - bid) * prc

        # adjust market strategy
        if self.strategy['qLearn'].fitted:
            for i in range(24):
                old_val = self.strategy['qLearn'].qus[self.strategy['qLearn'].sts[i],
                                                      int((self.strategy['actions'][i]-10)/10)]
                self.strategy['qLearn'].qus[self.strategy['qLearn'].sts[i], int((self.strategy['actions'][i]-10)/10)] \
                    = old_val + self.strategy['lr'] * (profit[i] - old_val)

        # adjust power generation
        self.portfolio.build_model(response=ask - bid)
        power_da, emission, fuel = self.portfolio.fix_planing()
        volume = self.portfolio.volume
        self.performance['adjustResult'] = tme.time() - start_time

        # Step 7: save adjusted results in influxdb
        # -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()

        df = pd.concat([pd.DataFrame.from_dict(self.portfolio.generation),
                        pd.DataFrame(data=dict(profit=profit, emissionAdjust=emission, fuelAdjust=fuel,
                                               volume=volume))], axis=1)
        df.index = pd.date_range(start=self.date, freq='60min', periods=len(df))
        self.connections['influxDB'].save_data(df, 'Areas', dict(typ=self.typ, agent=self.name, area=self.plz,
                                                                 timestamp='post_dayAhead'))

        self.performance['saveResult'] = tme.time() - start_time

        self.logger.info('After DayAhead market adjustment completed')
        self.logger.info('Next day scheduling started')

        # Step 8: retrain forecast methods and learning algorithm
        # -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()

        if self.strategy['delay'] <= 0:                                                         # offset factor start
            # collect data an retrain forecast method
            dem = self.connections['influxDB'].get_dem(self.date)                               # demand germany [MW]
            weather = self.connections['influxDB'].get_weather(self.geo, self.date, mean=True)  # mean weather germany
            prc_1 = self.connections['influxDB'].get_prc_da(self.date-pd.DateOffset(days=1))    # mcp yesterday [€/MWh]
            prc_7 = self.connections['influxDB'].get_prc_da(self.date-pd.DateOffset(days=7))    # mcp week before [€/MWh]
            for key, method in self.forecasts.items():
                method.collect_data(date=self.date, dem=dem, prc=prc[:24], prc_1=prc_1, prc_7=prc_7, weather=weather)
                method.counter += 1
                if method.counter >= method.collect:                                        # retrain forecast method
                    method.fit_function()
                    method.counter = 0

            # collect data for learning method
            self.strategy['qLearn'].counter += 1
            if self.strategy['qLearn'].counter >= self.strategy['qLearn'].collect:
                self.strategy['qLearn'].fit()
                self.strategy['qLearn'].counter = 0
            self.strategy['lr'] = max(self.strategy['lr']*0.99, 0.2)                # reduce learning rate during the simulation
            self.strategy['epsilon'] = max(0.99*self.strategy['epsilon'], 0.01)     # reduce random factor to find new opportunities
        else:
            self.strategy['delay'] -= 1

        df = pd.DataFrame(index=[pd.to_datetime(self.date)], data=self.portfolio.capacities)
        self.connections['influxDB'].save_data(df, 'Areas', dict(typ=self.typ, agent=self.name, area=self.plz))

        self.performance['nextDay'] = tme.time() - start_time

        df = pd.DataFrame(data=self.performance, index=[self.date])
        self.connections['influxDB'].save_data(df, 'Performance', dict(typ=self.typ, agent=self.name, area=self.plz))

        self.logger.info('Next day scheduling completed')


if __name__ == "__main__":

    args = parse_args()
    agent = PwpAgent(date='2018-01-01', plz=args.plz)
    agent.connections['mongoDB'].login(agent.name, False)
    # try:
    #     agent.run()
    # except Exception as e:
    #     print(e)
    # finally:
    #     agent.connections['influxDB'].influx.close()
    #     agent.connections['mongoDB'].mongo.close()
    #     if not agent.connections['connectionMQTT'].is_closed:
    #         agent.connections['connectionMQTT'].close()
    #     exit()
