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
from apps.qLearn_DayAhead import qLeran as daLearning


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--plz', type=int, required=False, default=81, help='PLZ-Agent')
    return parser.parse_args()

class resAgent(basicAgent):

    def __init__(self, date, plz):
        super().__init__(date=date, plz=plz, exchange='Market', typ='RES')
        # Development of the portfolio with the corresponding ee-systems
        self.logger.info('starting the agent')
        start_time = tme.time()
        self.portfolio = resPort(typ="RES")

        # Construction of the pv systems
        for key, value in self.ConnectionMongo.getPvParks().items():
            if value['typ'] != 'PV70':
                self.portfolio.capacities['solar'] += value['maxPower']
            else:
                self.portfolio.capacities['solar'] += value['maxPower'] * value['number']
            self.portfolio.addToPortfolio(key, {key: value})
        self.logger.info('PV Generation added')

        # Construction of the pv systems (h0)
        for key, value in self.ConnectionMongo.getPVs().items():
            self.portfolio.capacities['solar'] += value['PV']['maxPower'] * value['EEG']
            self.portfolio.addToPortfolio('Pv' + str(key), {'Pv' + str(key): value})
        self.logger.info('PV(H0) Generation added')

        # Construction Run River
        for key, value in self.ConnectionMongo.getRunRiver().items():
            self.portfolio.addToPortfolio('runRiver', {'runRiver': value})
            self.portfolio.capacities['water'] = value['maxPower']
        self.logger.info('Run River added')

        # Construction Biomass
        for key, value in self.ConnectionMongo.getBioMass().items():
            self.portfolio.addToPortfolio('bioMass', {'bioMass': value})
            self.portfolio.capacities['bio'] = value['maxPower']
        self.logger.info('Biomass Power Plants added')

        # Construction Windenergy
        for key, value in self.ConnectionMongo.getWind().items():
            self.portfolio.capacities['wind'] += value['maxPower']
            self.portfolio.addToPortfolio(key, {key: value})
        self.logger.info('Windenergy added')

        # Parameters for the trading strategy on the day-ahead market
        self.maxPrice = np.zeros(24)                                                        # maximal price of each hour
        self.minPrice = np.zeros(24)                                                        # minimal price of each hour
        self.actions = np.zeros(24)                                                         # different actions (slopes)
        self.espilion = 0.7                                                                 # factor to find new actions
        self.lr = 0.8                                                                       # learning rate
        self.qLearn = daLearning(self.ConnectionInflux, init=np.random.randint(5, 10 + 1))  # interval for learnings
        self.qLearn.qus *= 0.5 * (self.portfolio.capacities['wind'] + self.portfolio.capacities['solar'])
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
                    "tags": dict(typ='RES',                                                 # typ
                                 agent=self.name,                                           # name
                                 area=self.plz),                                            # area
                    "time": time.isoformat() + 'Z',
                    "fields": dict(capacitySolar=float(self.portfolio.capacities['solar']),
                                   capacityWind=float(self.portfolio.capacities['wind']),
                                   capacityWater=float(self.portfolio.capacities['water']),
                                   capacityBio=float(self.portfolio.capacities['bio']))
                }
            )
            time = time + pd.DateOffset(days=1)
        self.ConnectionInflux.saveData(json_body)

        timeDelta = tme.time() - start_time

        self.logger.info('setup of the agent completed in %s' % timeDelta)

    def optimize_dayAhead(self):
        """scheduling for the DayAhead market"""
        self.logger.info('DayAhead market scheduling started')


        # forecast and model build for the coming day
        # -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()

        weather = self.weatherForecast(self.date)
        price = self.priceForecast(self.date)
        demand = self.demandForecast(self.date)
        self.portfolio.setPara(self.date, weather, price, demand)
        self.portfolio.buildModel()

        self.perfLog('initModel', start_time)

        # standard optimzation --> returns power timeseries in [MW]
		# -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()

        power_dayAhead = np.asarray(self.portfolio.optimize(), np.float)

        # split power in eeg and direct marketing part

        powerDirect = np.zeros(24)
        powerEEG = np.zeros(24)
        for key, value in agent.portfolio.energySystems.items():
            # direct marketing
            if value['typ'] == 'wind' or value['typ'] == 'biomass' or value['typ'] == 'PVPark':
                powerDirect += value['model'].generation['wind'].reshape(-1)            # wind onshore
                powerDirect += value['model'].generation['solar'].reshape(-1)           # free area pv
                powerDirect += value['model'].generation['bio'].reshape(-1)             # biomass power plant
            # eeg marketing
            else:
                powerEEG += value['model'].generation['water'].reshape(-1)              # run river
                powerEEG += value['model'].generation['solar'].reshape(-1)              # pv systems before 2013

        self.perfLog('optModel', start_time)

        # save portfolio data in influxDB
		# -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()

        time = self.date
        json_body = []
        for i in self.portfolio.t:
            json_body.append(
                {
                    "measurement": 'Areas',
                    "tags": dict(typ='RES',                                             # typ
                                 agent=self.name,                                       # name
                                 area=self.plz,                                         # area
                                 timestamp='optimize_dayAhead'),                        # processing step
                    "time": time.isoformat() + 'Z',
                    "fields": dict(powerTotal=power_dayAhead[i],                        # maximal power         [MW]
                                   priceForcast=price['power'][i],                      # day Ahead forecast    [€/MWh]
                                   powerWind=self.portfolio.generation['wind'][i],      # total wind            [MW]
                                   powerBio=self.portfolio.generation['bio'][i],        # total bio             [MW]
                                   powerSolar=self.portfolio.generation['solar'][i],    # total pv              [MW]
                                   powerWater=self.portfolio.generation['water'][i],    # total run river       [MW]
                                   powerDirect=powerDirect[i],                          # direct power          [MW]
                                   powerEEG=powerEEG[i])                                # eeg power             [MW]
                }
            )
            time = time + pd.DateOffset(hours=self.portfolio.dt)
        self.ConnectionInflux.saveData(json_body)

        self.perfLog('saveScheduling', start_time)

        # build up oderbook and send to market (mongoDB)
		# -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()

        orderbook = dict()

        actions = np.random.randint(1, 8, 24) * 10
        prc = np.asarray(price['power']).reshape((-1, 1))

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

        # set minimal and maximal price in [€/MWh]
        self.maxPrice = prc.reshape((-1,)) + np.asarray([max(self.risk*v, 1) for v in var])                       # Maximalpreis      [€/MWh]
        self.minPrice = np.zeros_like(self.maxPrice)                                                              # Minimalpreis      [€/MWh]

        slopes = ((self.maxPrice - self.minPrice)/100) * np.tan((actions+10)/180*np.pi) # Preissteigung pro weitere MW

        for i in range(self.portfolio.T):
            quantity = [-1*powerEEG[i]]
            price = [-499.98]
            for _ in range(2, 102, 2):
                quantity.append(-1*(2/100 * powerDirect[i]))

            ub = self.maxPrice[i]
            lb = self.minPrice[i]
            slope = slopes[i]

            if slope > 0:
                for p in range(2, 102, 2):
                    price.append(float(min(slope * p + lb, ub)))
            else:
                for p in range(2, 102, 2):
                    price.append(float(min(-1*slope * p + ub, lb)))

            orderbook.update({'h_%s' % i: {'quantity': quantity, 'price': price, 'hour': i, 'name': self.name}})

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

        # save data and actions to learn from them
        self.qLearn.collectData(self.date, self.actions.reshape((24, 1)))

        # geplante Menge Day Ahead
        planing = self.ConnectionInflux.getPowerScheduling(self.date, self.name, 'optimize_dayAhead')

        # query the DayAhead results
        ask = self.ConnectionInflux.getDayAheadAsk(self.date, self.name)            # Angebotene Menge [MWh]
        bid = self.ConnectionInflux.getDayAheadBid(self.date, self.name)            # Nachgefragte Menge [MWh]
        price = self.ConnectionInflux.getDayAheadPrice(self.date)                   # MCP [€/MWh]

        # calculate the profit and the new power scheduling
        profit = [float((ask[i] - bid[i]) * price[i]) for i in range(24)]           # erzielte Erlöse
        power = np.asarray(ask - bid).reshape((-1,))

        self.portfolio.buildModel(response=power)
        self.portfolio.optimize()

        # Differenz aus Planung und Ergebnissen
        difference = np.asarray(planing).reshape((-1,)) - np.asarray(ask - bid).reshape((-1,))
        # Bestrafe eine nicht Vermarktung
        missed = [difference[i]*price[i] if price[i] > 0 else 0 for i in range(24)]

        # if a model is available, adjust the profit expectation according to the learning rate
        if self.qLearn.fitted:
            states = self.qLearn.getStates(self.date)
            for i in self.portfolio.t:
                oldValue = self.qLearn.qus[states[i], int((self.actions[i]-10)/10)]
                self.qLearn.qus[states[i], int((self.actions[i]-10)/10)] = oldValue + self.lr * (profit[i] - missed[i] - oldValue)
        else:
            states = [-1 for _ in self.portfolio.t]

        self.perfLog('optResults', start_time)

        # save energy system data in influxDB
        # -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()

        portfolioData = []
        time = self.date
        for i in self.portfolio.t:
            portfolioData.append(
                {
                    "measurement": 'Areas',
                    "tags": dict(typ='RES',                                             # Typ Erneuerbare Energien
                                 agent=self.name,                                       # Name des Agenten
                                 area=self.plz,                                         # Plz Gebiet
                                 state=int(states[i]),
                                 action=int(self.actions[i]),
                                 timestamp='post_dayAhead'),
                    "time": time.isoformat() + 'Z',
                    "fields": dict(powerTotal=power[i],                                 # gesamte Einspeisung           [MW]
                                   powerWind=self.portfolio.generation['wind'][i],      # gesamte Windeinspeisung       [MW]
                                   powerBio=self.portfolio.generation['bio'][i],        # gesamte Biomasseeinspeisung   [MW]
                                   powerSolar=self.portfolio.generation['solar'][i],    # gesamte PV-Einspeisung        [MW]
                                   powerWater=self.portfolio.generation['water'][i],    # gesamte Wasserkraft           [MW]
                                   profit=profit[i],                                    # erzielte Erlöse               [€]
                                   state=int(states[i]),
                                   action=int(self.actions[i]))
                }
            )
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

            # Ansappung der Statuspunkte des Energiesystems
            self.qLearn.counter += 1
            if self.qLearn.counter >= self.qLearn.collect:
                self.qLearn.fit()
                self.qLearn.counter = 0

            self.lr = max(self.lr*0.999, 0.4)
            self.espilion = max(0.99*self.espilion, 0.01)
        else:
            self.delay -= 1


        self.perfLog('nextDay', start_time)

        # -------------------------------------------------------------------------------------------------------------
        self.logger.info('Next day scheduling completed')


if __name__ == "__main__":

    args = parse_args()
    agent = resAgent(date='2018-01-01', plz=args.plz)
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
