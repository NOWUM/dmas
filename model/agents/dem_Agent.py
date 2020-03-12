# Importe
import argparse
import pandas as pd
import numpy as np
from agents.basic_Agent import agent as basicAgent
from aggregation.dem_Port import demPort
from apps.build_houses import Houses


# ----- Argument Parser for PLZ -----
def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--plz', type=int, required=False, default=60, help='PLZ-Agent')
    return parser.parse_args()


class demAgent(basicAgent):

    def __init__(self, date, plz, host='149.201.88.150'):
        super().__init__(date=date, plz=plz, host=host, exchange='DayAhead', typ='DEM')

        print('Start Building DEM %s \n' % plz)
        self.portfolio = demPort(typ="DEM")

        data, tech = self.mongoCon.getHouseholds(plz)
        # -- build up households
        if len(data) > 0:
            builder = Houses()
            housesBat = [builder.build(comp='PvBat') for _ in range(tech['battery'])]

            if tech['heatpump'] == min(tech['solar'] - tech['battery'], tech['heatpump']):
                housesWp = [builder.build(comp='PvWp') for _ in range(tech['heatpump'])]
                housesPv = [builder.build(comp='Pv') for _ in range(tech['solar'] - tech['heatpump'])]
            else:
                housesWp = [builder.build(comp='PvWp') for _ in range(tech['solar'] - tech['battery'])]
                housesPv = []

            demandP = np.sum([h[2] for h in housesBat]) + np.sum([h[2] for h in housesWp]) + np.sum(
                [h[2] for h in housesPv])
            for h in housesBat: self.portfolio.addToPortfolio(name=h[0], energysystem=h[1])
            del housesBat
            for h in housesWp: self.portfolio.addToPortfolio(name=h[0], energysystem=h[1])
            del housesWp
            for h in housesPv: self.portfolio.addToPortfolio(name=h[0], energysystem=h[1])
            del housesPv
            demandH0 = 1000*data['household'] - demandP

            name = 'plz_' + str(plz) + '_h0'
            self.portfolio.addToPortfolio(name, {name: {'demandP': np.round(demandH0, 2), 'typ': 'H0'}})
            name = 'plz_' + str(plz) + '_g0'
            self.portfolio.addToPortfolio(name, {name: {'demandP': np.round(1000*data['commercial'], 2), 'typ': 'G0'}})
            name = 'plz_' + str(plz) + '_rlm'
            self.portfolio.addToPortfolio(name, {name: {'demandP': np.round(1000*data['industrial'], 2), 'typ': 'RLM'}})

            print('Stop Building DEM %s \n' % plz)

    # ----- Balancing Handling -----
    def optimize_balancing(self):
        pass

    #----- Routine before day Ahead -----
    def optimize_dayAhead(self):
        # -- set parameter for optimization
        self.portfolio.setPara(self.date, self.weatherForecast(), self.priceForecast())

        # -- build up orderbook
        self.portfolio.buildModel()
        power = np.asarray(self.portfolio.optimize(), np.float)
        orders = dict(uuid=self.name, date=str(self.date))
        orders.update([(i, [(np.round(power[i] / 10**3, 2), 3000), (0,-300)]) for i in range(self.portfolio.T)])

       # -- send and save result
        if self.restCon.sendDayAhead(orders):
            json_body = []
            time = self.date
            for i in self.portfolio.t:
                json_body.append(
                    {
                        "measurement": 'Areas',
                        "tags": dict(agent=self.name, area=self.area,
                                     timestamp='optimize_dayAhead', typ='DEM'),
                        "time": time.isoformat() + 'Z',
                        "fields": dict(Power=power[i]/10**3)
                    }
                )
                time = time + pd.DateOffset(hours=self.portfolio.dt)
            self.influxCon.saveData(json_body)

    #----- Routine after day Ahead -----
    def post_dayAhead(self):
        # -- get Results from DayAhead Auction
        ask, bid, reward = self.influxCon.getDayAheadResult(self.date, self.name)
        power = ask - bid

        # -- save results
        json_body = []
        time = self.date
        for i in self.portfolio.t:
            json_body.append(
                {
                    "measurement": 'Areas',
                    "tags": dict(agent=self.name, area=self.area, timestamp='post_dayAhead', typ='DEM'),
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
        actual = np.asarray(self.portfolio.fixPlaning()/10**3, np.float).reshape((-1,))
        difference = np.asarray([schedule[i] - actual[i] for i in self.portfolio.t])
        power = actual

        # -- build up oderbook
        orders = dict(uuid=self.name, date=str(self.date))  # -- build & send result to market
        orders.update([(i, (difference[i])) for i in range(self.portfolio.T)])

        # -- save results
        if self.restCon.sendActuals(orders):
            json_body = []
            time = self.date
            for i in self.portfolio.t:
                json_body.append(
                    {
                        "measurement": 'Areas',
                        "tags": dict(agent=self.name, area=self.area, timestamp='optimize_actual', typ='DEM'),
                        "time": time.isoformat() + 'Z',
                        "fields": dict(Difference=difference[i], Power=power[i])
                    }
                )
                time = time + pd.DateOffset(hours=self.portfolio.dt)
            self.influxCon.saveData(json_body)

    # ----- Routine after Actual -----
    def post_actual(self):
        # -- fit functions and models for the next day
        self.nextDay()
        print('%s done' % self.date)

if __name__ == "__main__":

    args = parse_args()
    agent = demAgent(date='2019-01-01', plz=args.plz)
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

