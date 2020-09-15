# third party modules
import time as tme
import os
import argparse
import pandas as pd
import numpy as np

# model modules
os.chdir(os.path.dirname(os.path.dirname(__file__)))
from aggregation.res_Port import resPort
from agents.basic_Agent import agent as basicAgent


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--plz', type=int, required=False, default=85, help='PLZ-Agent')
    return parser.parse_args()


class ResAgent(basicAgent):

    def __init__(self, date, plz):
        super().__init__(date=date, plz=plz, exchange='Market', typ='RES')
        # Development of the portfolio with the corresponding ee-systems
        self.logger.info('starting the agent')
        start_time = tme.time()
        self.portfolio = resPort(typ="RES")

        # Construction Windenergy
        for key, value in self.connections['mongoDB'].getWind().items():
            self.portfolio.capacities['wind'] += value['maxPower']
            self.portfolio.addToPortfolio(key, {key: value})
        self.logger.info('Windenergy added')
        self.portfolio.mergeWind()

        # Construction of the pv systems (free area)
        for key, value in self.connections['mongoDB'].getPvParks().items():
            if value['typ'] != 'PV70':
                self.portfolio.capacities['solar'] += value['maxPower']
            else:
                self.portfolio.capacities['solar'] += value['maxPower'] * value['number']
            self.portfolio.addToPortfolio(key, {key: value})
        self.logger.info('PV(free area) Generation added')

        # Construction of the pv systems (h0)
        for key, value in self.connections['mongoDB'].getPVs().items():
            self.portfolio.capacities['solar'] += value['PV']['maxPower'] * value['EEG']
            self.portfolio.addToPortfolio('Pv' + str(key), {'Pv' + str(key): value})
        self.logger.info('PV(H0) Generation added')

        # Construction Run River
        for key, value in self.connections['mongoDB'].getRunRiver().items():
            self.portfolio.addToPortfolio('runRiver', {'runRiver': value})
            self.portfolio.capacities['water'] = value['maxPower']
        self.logger.info('Run River Power Plants added')

        # Construction Biomass
        for key, value in self.connections['mongoDB'].getBioMass().items():
            self.portfolio.addToPortfolio('bioMass', {'bioMass': value})
            self.portfolio.capacities['bio'] = value['maxPower']
        self.logger.info('Biomass Power Plants added')

        # If there are no power systems, terminate the agent
        if len(self.portfolio.energySystems) == 0:
            print('Number: %s No energy systems in the area' % plz)
            exit()

        df = pd.DataFrame(index=[pd.to_datetime(self.date)],
                          data=dict(capacitySolar=float(self.portfolio.capacities['solar']),
                                    capacityWind=float(self.portfolio.capacities['wind']),
                                    capacityWater=float(self.portfolio.capacities['water']),
                                    capacityBio=float(self.portfolio.capacities['bio'])))
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
        self.portfolio.setPara(self.date, weather, prices, demand)
        self.portfolio.buildModel()

        self.performance['initModel'] = tme.time() - start_time

        # Step 2: standard optimization --> returns power series in [MW]
        # -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()                                         # performance timestamp

        power_da = self.portfolio.optimize()                            # total portfolio power
        # split power in eeg and direct marketing part
        power_direct = agent.portfolio.generation['powerSolar'] + agent.portfolio.generation['powerWind']
        power_eeg = power_da - power_direct

        self.performance['optModel'] = tme.time() - start_time

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

        self.performance['saveSchedule'] = tme.time() - start_time

        # Step 4: build orders from optimization results
        # -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()

        order_book = dict()                                                     # dictionary to send the orders to the market
        self.strategy['maxPrice'] = prices['power']                             # set maximal price in [€/MWh]
        self.strategy['minPrice'] = np.zeros_like(self.strategy['maxPrice'])    # set minimal price in [€/MWh]
        # initialize learning algorithm
        self.strategy['qLearn'].collect_data(dem=demand, prc=prices['power'], weather=weather)
        # find best action according to the actual situation
        if self.strategy['qLearn'].fitted:              # if a model is available/trained find best actions
            opt_actions = self.strategy['qLearn'].get_actions()
            actions = [opt_actions[i] if self.strategy['epsilon'] < np.random.uniform(0, 1) else
                       np.random.randint(1, 8) for i in range(24)]
        else:                                           # else try random actions to get new hints
            actions = np.random.randint(1, 8, 24) * 10

        self.strategy['actions'] = np.asarray(actions).reshape(-1,)

        # build linear order function
        slopes = ((self.strategy['maxPrice'] - self.strategy['minPrice'])/100) * \
                 np.tan((self.strategy['actions']+10)/180*np.pi)

        # define volumes and prices for each hour
        for i in range(self.portfolio.T):
            # direct strategy
            volume = [(-2/100 * power_direct[i]) for _ in range(2, 102, 2)]
            if slopes[i] > 0:
                prices = [float(min(slopes[i] * p + self.strategy['minPrice'][i], self.strategy['maxPrice'][i]))
                          for p in range(2, 102, 2)]
            else:
                prices = [float(min(-slopes[i] * p + self.strategy['maxPrice'][i], self.strategy['minPrice'][i]))
                          for p in range(2, 102, 2)]
            # eeg strategy
            volume.insert(0, -power_eeg[i])
            prices.insert(0, -499.98)

            order_book.update({'h_%s' % i: {'quantity': volume, 'price': prices, 'hour': i, 'name': self.name}})

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
        ask = self.connections['influxDB'].get_ask_da(self.date, self.name)            # volume to buy
        bid = self.connections['influxDB'].get_bid_da(self.date, self.name)            # volume to sell
        prc = self.connections['influxDB'].get_prc_da(self.date)                       # market clearing price
        profit = (ask - bid) * prc

        # adjust market strategy
        if self.strategy['qLearn'].fitted:
            for i in self.portfolio.t:
                old_val = self.strategy['qLearn'].qus[self.strategy['qLearn'].sts[i],
                                                      int((self.strategy['actions'][i]-10)/10)]
                self.strategy['qLearn'].qus[self.strategy['qLearn'].sts[i], int((self.strategy['actions'][i]-10)/10)] \
                    = old_val + self.strategy['lr'] * (profit[i] - old_val)

        # adjust power generation
        self.portfolio.buildModel(response=ask-bid)
        _ = self.portfolio.optimize()

        self.performance['adjustResult'] = tme.time() - start_time

        # Step 7: save adjusted results in influxdb
        # -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()

        df = pd.concat([pd.DataFrame.from_dict(self.portfolio.generation),
                        pd.DataFrame(data=dict(profit=profit))], axis=1)
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
                method.collect_data(date=self.date, dem=dem, prc=prc, prc_1=prc_1, prc_7=prc_7, weather=weather)
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

        self.performance['nextDay'] = tme.time() - start_time

        df = pd.DataFrame(data=self.performance, index=[self.date])
        self.connections['influxDB'].save_data(df, 'Performance', dict(typ=self.typ, agent=self.name, area=self.plz))

        self.logger.info('Next day scheduling completed')


if __name__ == "__main__":

    args = parse_args()
    agent = ResAgent(date='2018-01-01', plz=args.plz)
    agent.connections['mongoDB'].login(agent.name, False)
    try:
        agent.run()
    except Exception as e:
        print(e)
    finally:
        agent.connections['influxDB'].influx.close()
        agent.connections['mongoDB'].mongo.close()
        if not agent.connections['connectionMQTT'].is_closed:
            agent.connections['connectionMQTT'].close()
        exit()
