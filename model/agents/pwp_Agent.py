# third party modules
import time as tme
import os
import argparse
import pandas as pd
import numpy as np

# model modules
os.chdir(os.path.dirname(os.path.dirname(__file__)))
from aggregation.pwp_Port import pwpPort
from agents.basic_Agent import agent as basicAgent
from apps.qLearn_DayAhead import qLeran as daLearning


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--plz', type=int, required=False, default=50, help='PLZ-Agent')
    return parser.parse_args()

class pwpAgent(basicAgent):

    def __init__(self, date, plz):
        super().__init__(date=date, plz=plz, exchange='Market', typ='PWP')
        # Development of the portfolio with the corresponding power plants and storages
        self.logger.info('starting the agent')
        start_time = tme.time()
        self.portfolio = pwpPort(typ='PWP', gurobi=True, T=48)

        # Construction power plants
        for key, value in self.ConnectionMongo.getPowerPlants().items():
            if value['maxPower'] > 1:
                self.portfolio.addToPortfolio(key, {key: value})
                self.portfolio.capacities['fossil'] += value['maxPower']                        # total power [MW]
                self.portfolio.capacities[value['fuel']] += value['maxPower']
        self.logger.info('Power Plants added')

        # Construction storages
        for key, value in self.ConnectionMongo.getStorages().items():
            self.portfolio.addToPortfolio(key, {key: value})
        self.logger.info('Storages added')

        # Parameters for the trading strategy on the day-ahead market
        self.maxPrice = np.zeros(24)                                                            # maximal price of each hour
        self.minPrice = np.zeros(24)                                                            # minimal price of each hour
        self.actions = np.zeros(24)                                                             # different actions (slopes)
        self.espilion = 0.7                                                                     # factor to find new actions
        self.lr = 0.8                                                                           # learning rate
        self.qLearn = daLearning(self.ConnectionInflux, init=np.random.randint(5, 10 + 1))      # interval for learning
        self.qLearn.qus *= 0.5 * self.portfolio.capacities['fossil']
        self.risk = np.random.choice([-5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5])
        self.logger.info('Parameters of the trading strategy defined')

        # If there are no power systems, terminate the agent
        if len(self.portfolio.energySystems) == 0:
            print('Number: %s No energy systems in the area' % plz)
            exit()

        # save capacities in influxDB
        json_body = []
        time = self.date
        for i in range(365):
            json_body.append(
                {
                    "measurement": 'Areas',
                    "tags": dict(typ='PWP',                                                 # typ
                                 agent=self.name,                                           # name
                                 area=self.plz),                                            # area
                    "time": time.isoformat() + 'Z',
                    "fields": dict(capacityNuc=float(self.portfolio.capacities['nuc']),
                                   capacityLignite=float(self.portfolio.capacities['lignite']),
                                   capacityCoal=float(self.portfolio.capacities['coal']),
                                   capacityGas=float(self.portfolio.capacities['gas']))
                }
            )
            time = time + pd.DateOffset(days=1)
        self.ConnectionInflux.saveData(json_body)

        timeDelta = tme.time() - start_time

        self.logger.info('setup of the agent completed in %s' % timeDelta)

    def optimize_dayAhead(self):
        """scheduling for the DayAhead market"""
        self.logger.info('DayAhead market scheduling started')
        start_time = tme.time()

        # forecasts for the coming day
        prices = self.priceForecast(self.date, 2)
        weather = self.weatherForecast(self.date, 2)
        demand = self.demandForecast(self.date, 2)
        self.portfolio.setPara(self.date, weather, prices, demand)
        self.portfolio.buildModel()

        # standard optimzation --> returns power timeseries in [MW] and var. cost in [€]
        power_dayAhead, emissionOpt, fuelOpt = self.portfolio.optimize()
        # calculate var. cost in [€/MWh] and set as minimal price
        costs = [(emissionOpt[i] + fuelOpt[i])/power_dayAhead[i] if power_dayAhead[i] != 0 else 0 for i in self.portfolio.t]
        self.minPrice = np.asarray(costs[:24]).reshape((-1,))

        # save energy system data in influxDB
        json_body = []
        for key, value in self.portfolio.energySystems.items():
            time = self.date
            power = value['model'].power
            volume = value['model'].volume
            for i in self.portfolio.t:
                json_body.append(
                    {
                        "measurement": 'Areas',
                        "tags": dict(typ='PWP',                                             # typ
                                     fuel=value['fuel'],                                    # fuel
                                     asset=key,                                             # energy system name
                                     agent=self.name,                                       # agent name
                                     area=self.plz,                                         # area
                                     timestamp='optimize_dayAhead'),                        # processing step
                        "time": time.isoformat() + 'Z',
                        "fields": dict(power=power[i],                                      # total power [MW]
                                       volume=volume[i])                                    # volume [MWh]
                    }
                )
                time = time + pd.DateOffset(hours=self.portfolio.dt)
        self.ConnectionInflux.saveData(json_body)

        # maximal power optimization --> returns power timeseries in [MW] and var. cost in [€]
        self.portfolio.buildModel(max_=True)
        power_max, emissionMax, fuelMax = self.portfolio.optimize()
        powerDelta = power_max - power_dayAhead
        powerDelta[powerDelta <= 0] = 1 * 10**-6
        # calculate var. cost in [€/MWh] and set as maximal price
        priceMax = (emissionMax + fuelMax) - (emissionOpt + fuelOpt) / powerDelta

        # save portfolio data in influxDB
        time = self.date
        json_body = []
        for i in self.portfolio.t:
            json_body.append(
                {
                    "measurement": 'Areas',
                    "tags": dict(typ='PWP',                                                 # typ
                                 agent=self.name,                                           # name
                                 area=self.plz,                                             # area
                                 timestamp='optimize_dayAhead'),                            # processing step
                    "time": time.isoformat() + 'Z',
                    "fields": dict(powerMax=power_max[i],                                   # maximal power      [MW]
                                   powerTotal=power_dayAhead[i],                            # total power        [MW]
                                   emissionOpt=emissionOpt[i],                              # optimal cost CO2   [€]
                                   fuelOpt=fuelOpt[i],                                      # optimal cost fuel  [€]
                                   emissionMax=emissionMax[i],                              # maximal cost CO2   [€]
                                   fuelMax=fuelMax[i],                                      # maximal cost fuel  [€]
                                   priceForcast=prices['power'][i],                         # day Ahead forecast [€/MWh]
                                   powerLignite=self.portfolio.generation['lignite'][i],    # total lignite      [MW]
                                   powerCoal=self.portfolio.generation['coal'][i],          # total coal         [MW]
                                   powerGas=self.portfolio.generation['gas'][i],            # total gas          [MW]
                                   powerNuc=self.portfolio.generation['nuc'][i],            # total nuc          [MW]
                                   powerStorage=self.portfolio.generation['water'][i])      # total storage      [MW]
                }
            )
            time = time + pd.DateOffset(hours=self.portfolio.dt)
        self.ConnectionInflux.saveData(json_body)

        # build up oderbook and send to market (mongoDB)
        orderbook = dict()

        actions = np.random.randint(1, 8, 24) * 10
        prc = np.asarray(prices['power'][:24]).reshape((-1, 1))

        # if a model is available/trained find best actions
        if self.qLearn.fitted:
            wnd = np.asarray(weather['wind'][:24]).reshape((-1, 1))                         # wind              [m/s]
            rad = np.asarray(weather['dir'][:24]).reshape((-1, 1))                          # direct rad.       [W/m²]
            tmp = np.asarray(weather['temp'][:24]).reshape((-1, 1))                         # temperatur        [°C]
            dem = np.asarray(demand[:24]).reshape((-1, 1))                                  # demand            [MW]
            actionsBest = self.qLearn.getAction(wnd, rad, tmp, dem, prc)

            for i in range(24):
                if self.espilion < np.random.uniform(0, 1):
                    actions[i] = actionsBest[i]
        self.actions = np.asarray(actions).reshape(24,)

        # calculation of the forecast quality
        var = np.sqrt(np.var(self.forecasts['price'].y, axis=0) * self.forecasts['price'].factor)
        if self.forecasts['price'].mcp.shape[0] > 0:
            var = np.sqrt(np.var(self.forecasts['price'].mcp, axis=0) * self.forecasts['price'].factor)

        # set maximal price in [€/MWh]
        self.maxPrice = prc.reshape((-1,)) + np.asarray([max(self.risk*v, 1) for v in var])

        delta = 0.
        nCounter = 0

        for i in range(24):
            if (self.maxPrice[i] > self.minPrice[i]) and power_dayAhead[i] > 0:
                delta += float((self.maxPrice[i] - self.minPrice[i]) * power_dayAhead[i])
            else:
                nCounter += 1
        if nCounter > 0:
            delta /= nCounter

        sortMCP = np.sort(self.maxPrice)
        maxBuy = sortMCP[:12][-1]
        minSell = sortMCP[12:][-1]

        for i in range(24):

            quantity = [float(-1 * (2 / 100 * (power_dayAhead[i]-self.portfolio.generation['water'][i])))
                        for _ in range(2, 102, 2)]

            mcp = self.maxPrice[i]
            cVar = self.minPrice[i]

            if (cVar > mcp) and power_dayAhead[i] > 0:
                if delta > 0:
                    lb = mcp - min(3*var[i]/power_dayAhead[i], delta/power_dayAhead[i])
                    slope = (mcp - lb) / 100 * np.tan((actions[i] + 10) / 180 * np.pi)
                    price = [float(min(slope * p + lb, mcp)) for p in range(2, 102, 2)]
                else:
                    price = [float(mcp) for _ in range(2, 102, 2)]
            else:
                lb = cVar
                slope = (mcp - lb) / 100 * np.tan((actions[i] + 10) / 180 * np.pi)
                price = [float(min(slope * p + lb, mcp)) for p in range(2, 102, 2)]
            if (power_max[i] > 0):
                quantity.append(float(-1 * power_max[i]))
                price.append(float(max(priceMax[i], self.maxPrice[i])))

            if self.portfolio.generation['water'][i] > 0:
                for p in range(2, 102, 2):
                    lb = minSell
                    slope = (mcp - lb) / 100 * np.tan(45/180 * np.pi)
                    price.append(float(min(slope * p + lb, mcp)))
                    quantity.append(float(-1 * (2 / 100 * self.portfolio.generation['water'][i])))
            if self.portfolio.generation['water'][i] < 0:
                for p in range(2, 102, 2):
                    lb = maxBuy
                    slope = (mcp - lb) / 100 * np.tan(45/180 * np.pi)
                    price.append(float(min(slope * p + lb, mcp)))
                    quantity.append(float(-1 * (2 / 100 * self.portfolio.generation['water'][i])))

            orderbook.update({'h_%s' % i: {'quantity': quantity, 'price': price, 'hour': i, 'name': self.name}})

        self.ConnectionMongo.setDayAhead(name=self.name, date=self.date, orders=orderbook)

        # save performance in influxDB
        timeDelta = tme.time() - start_time
        procssingPerfomance = [
            {
                "measurement": 'Performance',
                "tags": dict(typ='PWP',                         # typ
                             agent=self.name,                   # name
                             area=self.plz,                     # area
                             timestamp='optimize_dayAhead'),    # processing step
                "time": self.date.isoformat() + 'Z',
                "fields": dict(processingTime=timeDelta)

            }
        ]
        self.ConnectionInflux.saveData(procssingPerfomance)

        self.logger.info('DayAhead market scheduling completed in %s' % timeDelta)


    def post_dayAhead(self):
        """Scheduling after DayAhead Market"""
        self.logger.info('After DayAhead market scheduling started')
        start_time = tme.time()

        # save data and actions to learn from them
        self.qLearn.collectData(self.date, self.actions.reshape((24, 1)))

        # query the DayAhead results
        ask = self.ConnectionInflux.getDayAheadAsk(self.date, self.name, days=2)                # [MWh]
        bid = self.ConnectionInflux.getDayAheadBid(self.date, self.name, days=2)                # [MWh]
        price = self.ConnectionInflux.getDayAheadPrice(self.date, days=2)                       # [€/MWh]

        # calculate the profit and the new power scheduling
        profit = np.asarray([float((ask[i] - bid[i]) * price[i]) for i in self.portfolio.t])    # revenue for each hour
        self.portfolio.buildModel(response=ask-bid)
        power_dayAhead, emission, fuel = self.portfolio.fixPlaning()
        profit = profit.reshape((-1,)) - (emission + fuel).reshape((-1,))

        # if a model is available, adjust the profit expectation according to the learning rate
        if self.qLearn.fitted:
            states = self.qLearn.getStates(self.date)
            for i in range(24):
                oldValue = self.qLearn.qus[states[i], int((self.actions[i]-10)/10)]
                self.qLearn.qus[states[i], int((self.actions[i]-10)/10)] = oldValue + self.lr * (profit[i] - oldValue)
        else:
            states = [-1 for i in self.portfolio.t]

        # save energy system data in influxDB
        json_body = []
        for key, value in self.portfolio.energySystems.items():
            time = self.date
            power = value['model'].power
            volume = value['model'].volume
            for i in self.portfolio.t:
                json_body.append(
                    {
                        "measurement": 'Areas',
                        "tags": dict(typ='PWP',                                             # typ
                                     fuel=value['fuel'],                                    # fuel
                                     asset=key,                                             # energy system name
                                     agent=self.name,                                       # agent name
                                     area=self.plz,                                         # area
                                     timestamp='post_dayAhead'),                            # processing step
                        "time": time.isoformat() + 'Z',
                        "fields": dict(power=power[i],                                      # total power [MW]
                                       volume=volume[i])                                    # volume      [MWh]
                    }
                )
                time = time + pd.DateOffset(hours=self.portfolio.dt)
        self.ConnectionInflux.saveData(json_body)

        # save portfolio data in influxDB
        json_body = []
        time = self.date
        for i in self.portfolio.t[:24]:
            json_body.append(
                {
                    "measurement": 'Areas',
                    "tags": dict(typ='PWP',                                                 # typ
                                 agent=self.name,                                           # name
                                 area=self.plz,                                             # area
                                 state=int(states[i]),
                                 action=int(self.actions[i]),
                                 timestamp='post_dayAhead'),                                # processing step
                    "time": time.isoformat() + 'Z',
                    "fields": dict(powerTotal=power_dayAhead[i],                            # total power     [MW]
                                   emissionCost=self.portfolio.emisson[i],                  # cost  CO2       [€]
                                   fuelCost=self.portfolio.fuel[i],                         # cost  fuel      [€]
                                   powerLignite=self.portfolio.generation['lignite'][i],    # total lignite   [MW]
                                   powerCoal=self.portfolio.generation['coal'][i],          # total coal      [MW]
                                   powerGas=self.portfolio.generation['gas'][i],            # total gas       [MW]
                                   powerNuc=self.portfolio.generation['nuc'][i],            # total nuc       [MW]
                                   powerStorage=self.portfolio.generation['water'][i],      # total Storage   [MW]
                                   profit=profit[i],                                        # profit          [€]
                                   state=int(states[i]),
                                   action=int(self.actions[i]))
                }
            )
            time = time + pd.DateOffset(hours=self.portfolio.dt)
        self.ConnectionInflux.saveData(json_body)

        # save performance in influxDB
        timeDelta = tme.time() - start_time
        procssingPerfomance = [
            {
                "measurement": 'Performance',
                "tags": dict(typ='PWP',                         # typ
                             agent=self.name,                   # name
                             area=self.plz,                     # area
                             timestamp='post_dayAhead'),        # processing step
                "time": self.date.isoformat() + 'Z',
                "fields": dict(processingTime=timeDelta)

            }
        ]
        self.ConnectionInflux.saveData(procssingPerfomance)

        self.logger.info('After DayAhead market scheduling completed in %s' % timeDelta)

        # scheduling for the next day
        self.logger.info('Next day scheduling started')

        start_time = tme.time()

        if self.delay <= 0:
            for key, method in self.forecasts.items():
                if key != 'weather':
                    method.collectData(self.date)
                    method.counter += 1
                    if method.counter >= method.collect:
                        method.fitFunction()
                        method.counter = 0

            # Ansappung der Statuspunkte des Energiesystems
            self.qLearn.counter += 1
            if self.qLearn.counter >= self.qLearn.collect:
                self.qLearn.fit()
                self.qLearn.counter = 0

            self.lr = max(self.lr*0.9, 0.4)                                 # Lernrate * 0.9 (Annahme Markt ändert sich
                                                                            # Zukunft nicht mehr so schnell)
            self.espilion = max(0.99*self.espilion, 0.01)                   # Epsilion * 0.9 (mit steigender Simulationdauer
                                                                            # sind viele Bereiche schon bekannt
        else:
            self.delay -= 1

        # save performance in influxDB
        timeDelta = tme.time() - start_time
        procssingPerfomance = [
            {
                "measurement": 'Performance',
                "tags": dict(typ='PWP',                         # typ
                             agent=self.name,                   # name
                             area=self.plz,                     # area
                             timestamp='nextDay_scheduling'),   # processing step
                "time": self.date.isoformat() + 'Z',
                "fields": dict(processingTime=timeDelta)

            }
        ]
        self.ConnectionInflux.saveData(procssingPerfomance)

        self.logger.info('Next day scheduling completed in %s' % timeDelta)


if __name__ == "__main__":

    args = parse_args()
    agent = pwpAgent(date='2018-01-01', plz=args.plz)
    agent.ConnectionMongo.login(agent.name, True)
    try:
        agent.run_agent()
    except Exception as e:
        print(e)
    finally:
        agent.ConnectionInflux.influx.close()
        agent.ConnectionMongo.logout(agent.name)
        agent.ConnectionMongo.mongo.close()
        if not agent.connection.is_closed:
            agent.connection.close()
        exit()
