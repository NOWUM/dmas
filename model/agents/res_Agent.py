# Importe
import os
os.chdir(os.path.dirname(os.path.dirname(__file__)))
import argparse
import pandas as pd
import numpy as np
from aggregation.res_Port import resPort
from agents.basic_Agent import agent as basicAgent


# ----- Argument Parser for PLZ -----
def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--plz', type=int, required=False, default=25, help='PLZ-Agent')
    parser.add_argument('--mongo', type=str, required=False, default='149.201.88.150', help='IP MongoDB')
    parser.add_argument('--influx', type=str, required=False, default='149.201.88.150', help='IP InfluxDB')
    parser.add_argument('--market', type=str, required=False, default='149.201.88.150', help='IP Market')
    return parser.parse_args()

class resAgent(basicAgent):

    def __init__(self, date, plz, mongo='149.201.88.150', influx='149.201.88.150', market='149.201.88.150'):
        super().__init__(date=date, plz=plz, mongo=mongo, influx=influx, market=market, exchange='DayAhead', typ='RES')

        print('Start Building RES %s \n' %plz)
        self.portfolio = resPort(typ='RES', gurobi=False)

        data, tech = self.mongoCon.getWindOn(plz)
        # -- build up wind turbines
        if len(data) > 0:
            for i in range(len(data['power'])):
                name = 'plz_' + str(plz) + '_windOn_' + str(i)
                typ = data['typ'][i]
                generator = dict(P=data['power'][i], typ='wind')
                generator.update(tech[str(typ)])
                self.portfolio.addToPortfolio(name, {name : generator})
            self.portfolio.Cap_Wind = sum(data['power']) / 1000

        data, tech = self.mongoCon.getPvParks(plz)
        # -- build up solar parks
        if len(data) > 0:
            for i in range(len(data['power'])):
                name = 'plz_' + str(plz) + '_solar_' + str(i)
                generator = dict(peakpower=data['power'][i], typ='solar', eta=0.15, area=10.5)
                self.portfolio.addToPortfolio(name, {name : generator})
            self.portfolio.Cap_Solar = sum(data['power']) / 1000

        print('Stop Building RES %s \n' % plz)

    def getStates(self):

        self.portfolio.setPara(weather=self.weatherForecast(), date=self.date, prices={})
        self.portfolio.buildModel()
        power = np.asarray(self.portfolio.optimize(), np.float) * 0.95

        states = [[min(power[i:i + 4]) / 2 for i in range(0, 24, 4)],
                  [min(power[i:i + 4]) / 2 for i in range(0, 24, 4)]]

        return [np.mean(power), np.min(power)],states

    # ----- Balancing Handling -----
    def optimize_balancing(self):
        # -- set parameter for optimization
        self.portfolio.setPara(self.date, self.weatherForecast(), self.priceForecast(), self.demandForecast())

        # -- save the status for learning
        self.intelligence['Balancing'].input['weather'].append([x[1] for x in agent.portfolio.weather.items()])
        self.intelligence['Balancing'].input['dates'].append(self.portfolio.date)
        self.intelligence['Balancing'].input['prices'].append(self.portfolio.prices['power'])
        self.intelligence['Balancing'].input['states'].append(self.getStates()[0])

        # -- get best split between dayAhead and balancing market
        if self.intelligence['Balancing'].randomPoint:
            xopt, _ = self.intelligence['Balancing'].getAction([x[1] for x in agent.portfolio.weather.items()], self.getStates()[0],
                                                               self.portfolio.date, self.priceForecast()['power'])
            xopt = np.asarray(xopt).reshape((6,-1))

        # -- build up orderbook
        actions = []
        orders = dict(uuid=self.name, date=str(self.date))
        for i in range(6):
            # -- amount of power
            a = np.random.uniform(low=0, high=1)
            # -- power prices
            powerPricePos = np.random.uniform(low=20, high=500)
            powerPriceNeg = np.random.uniform(low=20, high=500)
            # -- energy prices
            energyPricePos = np.random.uniform(low=20, high=500)
            energyPriceNeg = np.random.uniform(low=20, high=500)
            if self.intelligence['Balancing'].randomPoint:
                powerPricePos = xopt[i,1]
                powerPriceNeg = xopt[i,2]
                energyPricePos = xopt[i,3]
                energyPriceNeg = xopt[i,4]
            # -- append actions
            actions.append([a, powerPricePos, energyPricePos, powerPriceNeg, energyPriceNeg])

            # -- build orders
            orders.update({str(i) + '_pos': (np.round(self.getStates()[1][0][i] * a, 0), powerPricePos, energyPricePos)})
            orders.update({str(i) + '_neg': (np.round(self.getStates()[1][1][i] * a, 0), powerPriceNeg, energyPriceNeg)})

        # -- save actions for learning
        self.intelligence['Balancing'].input['actions'].append(actions)
        # -- send orderbook to market plattform
        self.restCon.sendBalancing(orders)

    # ----- Day Ahead Handling -----
    def optimize_dayAhead(self):
        # -- get requirements from balancing
        pos, neg, _ = self.influxCon.getBalPowerResult(self.date, self.name)

        # -- set parameter for optimization
        self.portfolio.setPara(self.date, self.weatherForecast(), self.priceForecast(), self.demandForecast(), pos, neg)

        # -- save the status for learning
        self.intelligence['DayAhead'].input['weather'].append([x[1] for x in agent.portfolio.weather.items()])
        self.intelligence['DayAhead'].input['dates'].append(self.portfolio.date)
        self.intelligence['DayAhead'].input['prices'].append(self.portfolio.prices['power'])
        self.intelligence['DayAhead'].input['demand'].append(self.portfolio.demand)
        self.intelligence['DayAhead'].input['states'].append([self.portfolio.posBalPower, self.portfolio.negBalPower])

        # -- build up orderbook
        self.portfolio.buildModel()
        power = np.asarray(self.portfolio.optimize(), np.float)

        marginal = np.random.uniform(-20, 20, 24)
        if self.intelligence['DayAhead'].randomPoint:
            marginal, _ = self.intelligence['DayAhead'].getAction([x[1] for x in agent.portfolio.weather.items()],
                                                               [self.portfolio.posBalPower, self.portfolio.negBalPower],
                                                               self.portfolio.date, self.portfolio.prices['power'], self.portfolio.demand)
        orders = dict(uuid=self.name, date=str(self.date))
        orders.update([(i, [(float(-1 * self.portfolio.negBalPower[i]), marginal[i]),
                            (float(-1 * (power[i] - self.portfolio.posBalPower[i])), self.portfolio.prices['power'][i] + marginal[i])])
                       for i in range(self.portfolio.T)])
        # -- save marginals for learning
        self.intelligence['DayAhead'].input['actions'].append(marginal)

        # -- send and save result
        if self.restCon.sendDayAhead(orders):
            json_body = []
            time = self.date
            for i in self.portfolio.t:
                json_body.append(
                    {
                        "measurement": 'Areas',
                        "tags": dict(agent=self.name, area=self.area,
                                     timestamp='optimize_dayAhead', typ='RES'),
                        "time": time.isoformat() + 'Z',
                        "fields": dict(Power=power[i])
                    }
                )
                time = time + pd.DateOffset(hours=self.portfolio.dt)
            self.influxCon.saveData(json_body)

    # ----- Routine after day Ahead -----
    def post_dayAhead(self):
        # -- get Results from DayAhead Auction
        ask, bid, reward = self.influxCon.getDayAheadResult(self.date, self.name)

        # -- save reward for learning
        self.intelligence['DayAhead'].input['Qs'].append(reward)

        # -- minimize difference between dayAhead plan and result
        power = ask-bid
        _, negativePower, _ = self.influxCon.getBalPowerResult(self.date, self.name)
        power = [max(power[i], negativePower[i]) for i in self.portfolio.t]

        # -- save results
        json_body = []
        time = self.date
        for i in self.portfolio.t:
            json_body.append(
                {
                    "measurement": 'Areas',
                    "tags": dict(agent=self.name, area=self.area, timestamp='post_dayAhead', typ='RES'),
                    "time": time.isoformat() + 'Z',
                    "fields": dict(Power=power[i])
                }
            )
            time = time + pd.DateOffset(hours=self.portfolio.dt)
        self.influxCon.saveData(json_body)

    # ----- Actual Handling -----
    def optimize_actual(self):
        # -- get DayAhead requirements
        schedule = self.influxCon.getDayAheadSchedule(self.date, self.name)

        # -- difference for balancing energy
        actual = self.portfolio.fixPlaning()
        difference = np.asarray([(schedule[i] - actual[i] if schedule[i] > 0 else 0.00) for i in self.portfolio.t])
        power = np.asarray([(actual[i] if schedule[i] > 0 else 0) for i in self.portfolio.t])

        # -- build up oderbook
        orders = dict(uuid=self.name, date=str(self.date))
        orders.update([(i, (difference[i])) for i in range(self.portfolio.T)])

        # -- send and save results
        if self.restCon.sendActuals(orders):
            json_body = []
            time = self.date
            for i in self.portfolio.t:
                json_body.append(
                    {
                        "measurement": 'Areas',
                        "tags": dict(agent=self.name, area=self.area, timestamp='optimize_actual', typ='RES'),
                        "time": time.isoformat() + 'Z',
                        "fields": dict(Power=power[i], Difference=difference[i])
                    }
                )
                time = time + pd.DateOffset(hours=self.portfolio.dt)
            self.influxCon.saveData(json_body)

    # ----- Routine after Actual -----
    def post_actual(self):
        # -- get chashflows
        _, _, rewardPower = self.influxCon.getBalPowerResult(self.date, self.name)
        pos, neg, rewardEnergy = self.influxCon.getBalEnergyResult(self.date, self.name)

        # -- save profit for learning
        self.intelligence['Balancing'].input['Qs'].append(rewardPower + rewardEnergy)

        # -- provision of balancing power
        power = self.influxCon.getActualPlan(self.date,self.name) + pos - neg

        # -- save result
        time = self.date
        json_body = []
        for i in self.portfolio.t:
            json_body.append(
                {
                    "measurement": 'Areas',
                    "tags": dict(agent=self.name, area=self.area, timestamp='post_actual', typ='RES'),
                    "time": time.isoformat() + 'Z',
                    "fields": dict(Power=power[i])
                }
            )
            time = time + pd.DateOffset(hours=self.portfolio.dt)
        self.influxCon.saveData(json_body)

        # -- fit functions and models for the next day
        self.nextDay()

        print('%s done' % self.date)

if __name__ == "__main__":

    args = parse_args()
    agent = resAgent(date='2019-01-01', plz=args.plz, mongo=args.mongo, influx=args.influx, market=args.market)
    agent.restCon.login(agent.name, agent.typ)
    try:
        agent.run_agent()
    except Exception as e:
        print(e)
    finally:
        try:
            agent.restCon.logout(agent.name)
            agent.receive.close()
            agent.influxCon.influx.close()
            agent.mongoCon.mongo.close()
        except:
            print('services already closed')
