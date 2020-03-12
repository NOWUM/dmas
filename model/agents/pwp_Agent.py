# Importe
import argparse
import pandas as pd
import numpy as np
from aggregation.pwp_Port import pwpPort
from agents.basic_Agent import agent as basicAgent


# ----- Argument Parser for PLZ -----
def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--plz', type=int, required=False, default=33, help='PLZ-Agent')
    return parser.parse_args()

class pwpAgent(basicAgent):

    def __init__(self, date, plz, host='149.201.88.150'):
        super().__init__(date=date, plz=plz, host=host, exchange='DayAhead', typ='PWP')

        print('Start Building PWP %s \n' %plz)
        self.portfolio = pwpPort(typ='PWP', gurobi=True)

        data, tech = self.mongoCon.getPowerPlants(plz)
        # -- build up power plants
        if len(data) > 0:
            for i in range(len(data['power'])):
                fuel = data['fuel'][i]
                typ = data['typ'][i]
                p = data['power'][i]
                t = tech[str(fuel) + '_%i' %typ]
                name = 'plz_' + str(plz) + '_block_' + str(i)
                block = {name: dict(typ='konv', fuel=fuel, powerMax=np.round(p,1), powerMin=np.round(p * t['out_min']/100,1), eta=t['eta'],
                                    chi='0.15', P0=np.round(p * t['out_min']/100,1), stopTime=t['down_min'],runTime=t['up_min'],
                                    on=t['up_min'], gradP=int(t['gradient']/100 * 4 * p), gradM=int(t['gradient']/100 * 4 * p), heat=[])
                        }
                self.portfolio.addToPortfolio(name, block)
            self.portfolio.pwpCap = sum(data['power'])

        data, tech = self.mongoCon.getStorages(plz)
        # -- build up storages
        if len(data) > 0:
            # ----- Add Energy Systems (Storages) -----
            data = self.mongoTable.find({"_id": plz})[0]['storage'][0]
            for i in range(len(data['power'])):
                power = data['power'][i]
                energy = data['energy'][i]
                name = 'plz_' + str(plz) + '_storage_' + str(i)
                storage = {name: {'typ': 'storage', 'VMin': 0, 'VMax': energy,'P+_Max': power, 'P-_Max': power,
                                  'P+_Min': 0, 'P-_Min': 0, 'V0': energy / 2, 'eta+': 0.85, 'eta-': 0.80}}
                self.portfolio.addToPortfolio(name, storage)

        print('Stop Building PWP %s \n' % plz)

    def getSates(self):

        states = [0, 0]
        for _, value in self.portfolio.energySystems.items():
            if value['typ'] == 'konv':
                if value['P0'] == 0:
                    states[0] += 0
                    states[1] += 0
                else:
                    states[0] += min(value['powerMax'] - value['P0'], value['gradP'])
                    states[1] += max(min(value['P0'] - value['powerMin'], value['gradM']), 0)

        return states

    # ----- Balancing Handling -----
    def optimize_balancing(self):
        # -- set parameter for optimization
        self.portfolio.setPara(self.date, self.weatherForecast(), self.priceForecast(), self.demandForecast())

        # -- save the status for learning
        self.intelligence['Balancing'].input['weather'].append([x[1] for x in agent.portfolio.weather.items()])
        self.intelligence['Balancing'].input['dates'].append(self.portfolio.date)
        self.intelligence['Balancing'].input['prices'].append(self.portfolio.prices['power'])
        self.intelligence['Balancing'].input['states'].append(self.getSates())

        # -- get best split between dayAhead and balancing market
        if self.intelligence['Balancing'].randomPoint:
            xopt, _ = self.intelligence['Balancing'].getAction([x[1] for x in agent.portfolio.weather.items()], self.getSates(),
                                                               self.portfolio.date, self.portfolio.prices['power'])
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
            orders.update({str(i) + '_pos': (np.round(self.getSates()[0] * a, 0), powerPricePos, energyPricePos)})
            orders.update({str(i) + '_neg': (np.round(self.getSates()[1] * a, 0), powerPriceNeg, energyPriceNeg)})

        # -- save actions for learning
        self.intelligence['Balancing'].input['actions'].append(actions)
        # -- send orderbook to market plattform
        self.restCon.sendBalancing(orders)

    # ----- Day Ahead Handling -----
    # ------------------------------------------------------------------------------------------------------
    def optimize_dayAhead(self):
        # -- get requirements from balancing
        pos, neg, _ = self.influxCon.getBalPowerResult(self.date, self.name)

        # -- set parameter for optimization
        self.portfolio.setPara(self.date, self.weatherForecast(), self.priceForecast(), self.demandForecast(),pos, neg)

        # -- save the status for learning
        self.intelligence['DayAhead'].input['weather'].append([x[1] for x in agent.portfolio.weather.items()])
        self.intelligence['DayAhead'].input['dates'].append(self.portfolio.date)
        self.intelligence['DayAhead'].input['prices'].append(self.portfolio.prices['power'])
        self.intelligence['DayAhead'].input['demand'].append(self.portfolio.demand)
        self.intelligence['DayAhead'].input['states'].append([self.portfolio.posBalPower, self.portfolio.negBalPower])

        # -- build up orderbook
        in_ = self.priceForecast()
        order = dict()

        # -- minimal price for negative balancing power
        in_['power'] = -50*np.ones(self.portfolio.T)
        self.portfolio.setPara(date=self.date, weather=self.weatherForecast(), prices=in_, demand=self.demandForecast(),
                               posBalPower=pos, negBalPower=neg)
        self.portfolio.buildModel()
        power0 = np.asarray(self.portfolio.optimize(), np.float)
        order.update({'min': [power0, in_['power']]})

        # -- 0.5 * mean price < mean price < 1.5 * mean price
        for i in np.arange(0.50, 1.55, 0.05):
            factor = np.round(i, 2)
            in_['power'] = self.priceForecast()['power'] * factor
            self.portfolio.setPara(date=self.date, weather=self.weatherForecast(), prices=in_, demand=self.demandForecast(),
                                   posBalPower=pos, negBalPower=neg)
            self.portfolio.buildModel()
            power1 = np.asarray(self.portfolio.optimize(), np.float)
            order.update({factor: [power1-power0, in_['power']]})
            power0 = power1

        # -- max power (10 * mean price)
        in_ = self.priceForecast()
        in_['power'] = np.asarray([min(in_['power'][i] * 10, 3000) for i in self.portfolio.t], np.float)
        self.portfolio.setPara(date=self.date, weather=self.weatherForecast(), prices=in_, demand=self.demandForecast(),
                               posBalPower=pos, negBalPower=neg)
        self.portfolio.buildModel()
        power1 = np.asarray(self.portfolio.optimize(), np.float)
        order.update({'max': [power1-power0, in_['power']]})

        # -- select best marginals
        marginal = np.random.uniform(-2, 10, 24)
        if self.intelligence['DayAhead'].randomPoint:
            marginal, _ = self.intelligence['DayAhead'].getAction([x[1] for x in agent.portfolio.weather.items()],
                                                               [self.portfolio.posBalPower, self.portfolio.negBalPower],
                                                               self.portfolio.date, in_['power'], self.portfolio.demand)
        # -- save marginals for learning
        self.intelligence['DayAhead'].input['actions'].append(marginal)

        # -- send orderbook to market plattform
        keys = ['min', 0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90, 0.95, 1.00, 1.05, 1.10, 1.15, 1.20, 1.25,
                1.30, 1.35, 1.40, 1.45, 1.50, 'max']
        orders = dict(uuid=self.name, date=str(self.date))
        orders.update([(i, [(float(-1 * order[k][0][i]), float(order[k][1][i] + marginal[i]))
                            for k in keys]) for i in range(24)])

        # -- send and save result
        if self.restCon.sendDayAhead(orders):
            json_body = []
            for key, value in self.portfolio.energySystems.items():
                time = self.date
                power = [self.portfolio.m.getVarByName('P' + '_%s[%i]' % (key, i)).x for i in self.portfolio.t]
                volume = np.zeros_like(power)
                if value['typ'] == 'storage':
                    volume = [self.portfolio.m.getVarByName('V' + '_%s[%i]' % (key, i)).x for i in self.portfolio.t]
                for i in self.portfolio.t:
                    json_body.append(
                        {
                            "measurement": 'Areas',
                            "tags": dict(plant=value['typ'], asset=key, agent=self.name, area=self.area,
                                         timestamp='optimize_dayAhead', typ='PWP'),
                            "time": time.isoformat() + 'Z',
                            "fields": dict(Power=power[i], Volume=volume[i])
                        }
                    )
                    time = time + pd.DateOffset(hours=self.portfolio.dt)
            self.influxCon.saveData(json_body)

    # ----- Routine after day Ahead -----
    def post_dayAhead(self):
        # -- get Results from DayAhead Auction
        ask, bid, reward = self.influxCon.getDayAheadResult(self.date, self.name)

        # -- save profit for learning
        self.intelligence['DayAhead'].input['Qs'].append(reward)

        # -- minimize difference between dayAhead plan and result
        self.portfolio.buildModel(response=ask-bid)
        power = self.portfolio.fixPlaning()

        # -- save results
        json_body = []
        time = self.date
        for i in self.portfolio.t:
            json_body.append(
                {
                    "measurement": 'Areas',
                    "tags": dict(agent=self.name, area=self.area, timestamp='post_dayAhead', typ='PWP'),
                    "time": time.isoformat() + 'Z',
                    "fields": dict(Power=power[i])
                }
            )
            time = time + pd.DateOffset(hours=self.portfolio.dt)
        self.influxCon.saveData(json_body)

    # ----- Actual Handling -----
    def optimize_actual(self):
        # -- get Results from DayAhead Auction
        ask, bid, _ = self.influxCon.getDayAheadResult(self.date, self.name)
        schedule = self.influxCon.getDayAheadSchedule(self.date, self.name)

        # -- difference for balancing energy
        difference = schedule-(ask-bid)

        # -- build up oderbook
        orders = dict(uuid=self.name, date=str(self.date))
        orders.update([(i, (difference[i])) for i in range(self.portfolio.T)])

        # -- send and save results
        if self.restCon.sendActuals(orders):
            time = self.date
            json_body = []
            for i in self.portfolio.t:
                json_body.append(
                    {
                        "measurement": 'Areas',
                        "tags": dict(agent=self.name, area=self.area, timestamp='optimize_actual', typ='PWP'),
                        "time": time.isoformat() + 'Z',
                        "fields": dict(Difference=difference[i], Power=schedule[i])
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
        self.portfolio.setPara(self.date, self.weatherForecast(), self.priceForecast(), self.demandForecast(),
                               np.zeros(self.portfolio.T), np.zeros(self.portfolio.T))
        self.portfolio.buildModel(response=self.influxCon.getActualPlan(self.date, self.name) + pos - neg)
        power = self.portfolio.fixPlaning()

        # -- save result
        time = self.date
        json_body = []
        for i in self.portfolio.t:
            json_body.append(
                {
                    "measurement": 'Areas',
                    "tags": dict(agent=self.name, area=self.area, timestamp='post_actual', typ='PWP'),
                    "time": time.isoformat() + 'Z',
                    "fields": dict(Power=power[i])
                }
            )
            time = time + pd.DateOffset(hours=self.portfolio.dt)
        self.influxCon.saveData(json_body)

        # -- fit functions and models for the next day
        self.nextDay()

        print('%s done' %self.date)

if __name__ == "__main__":

    args = parse_args()
    agent = pwpAgent(date='2019-01-01', plz=args.plz)
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
