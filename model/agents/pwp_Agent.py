import os
os.chdir(os.path.dirname(os.path.dirname(__file__)))
from aggregation.pwp_Port import pwpPort
from agents.basic_Agent import agent as basicAgent
from apps.qLearn_DayAhead import qLeran as daLearning
import logging
import argparse
import pandas as pd
import numpy as np


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--plz', type=int, required=False, default=44, help='PLZ-Agent')
    parser.add_argument('--mongo', type=str, required=False, default='149.201.88.150', help='IP MongoDB')
    parser.add_argument('--influx', type=str, required=False, default='149.201.88.150', help='IP InfluxDB')
    parser.add_argument('--market', type=str, required=False, default='149.201.88.150', help='IP Market')
    parser.add_argument('--dbName', type=str, required=False, default='MAS_XXXX', help='Name der Datenbank')
    return parser.parse_args()

class pwpAgent(basicAgent):

    def __init__(self, date, plz, mongo='149.201.88.150', influx='149.201.88.150', market='149.201.88.150', dbName='MAS_XXXX'):
        super().__init__(date=date, plz=plz, mongo=mongo, influx=influx, market=market, exchange='Market', typ='PWP', dbName=dbName)

        logging.info('Start des Agenten')

        # Aufbau des Portfolios mit den enstprechenden Kraftwerken und Speichern
        self.portfolio = pwpPort(typ='PWP', gurobi=True)                    # Verwendung von Gurobi

        # Einbindung der Kraftwerksdaten aus der MongoDB und dem TechFile in ./data
        totalPower = 0
        powerPlants = self.ConnectionMongo.getPowerPlants(plz)
        for key, value in powerPlants.items():
            if value['maxPower'] > 1:
                self.portfolio.addToPortfolio(key, {key: value})
                totalPower += value['maxPower']                                 # Gesamte Kraftwerksleitung  in [MW]
        self.portfolio.capacities['fossil'] = totalPower

        logging.info('Kraftwerke hinzugefügt')

        # Einbindung der Speicherdaten aus der MongoDB
        storages = self.ConnectionMongo.getStorages(plz)
        for key, value in storages.items():
            self.portfolio.addToPortfolio(key, {key: value})

        logging.info('Speicher hinzugefügt')

        # Parameter für die Handelsstrategie am Day Ahead Markt
        self.maxPrice = []                                                  # Maximalgebot
        self.minPrice = []                                                  # Minimalgenbot
        self.actions = np.zeros(24)                                         # Steigung der Gebotsgeraden für jede Stunde
        self.espilion = 0.8                                                 # Faktor zum Abtasten der Möglichkeiten
        self.lr = 0.8                                                       # Lernrate des Q-Learning-Einsatzes
        self.qLearn = daLearning(self.ConnectionInflux, init=np.random.randint(5, 10 + 1))             # Lernalgorithmus im x Tage Rythmus
        self.qLearn.qus = self.qLearn.qus * self.portfolio.capacities['fossil']
        logging.info('Parameter der Handelsstrategie festgelegt')

        logging.info('Aufbau des Agenten abgeschlossen')

    def optimize_balancing(self):
        # TODO Überarbeiten der Gebotsstrategie --> Biete immer 30 % der Verfügbaren Menge zu Random Preisen
        """Einsatzplanung für den Regelleistungsmarkt"""
        orderbook = dict()                                                  # Oderbook für alle Blöcke (Stunde 1-6)

        states = [0, 0]
        for _, value in self.portfolio.energySystems.items():
            if value['typ'] == 'konv':
                if value['P0'] == 0:
                    states[0] += 0
                    states[1] += 0
                else:
                    states[0] += min(value['maxPower'] - value['P0'], value['gradP'])
                    states[1] += max(min(value['P0'] - value['powerMin'], value['gradM']), 0)

        for i in range(6):
            a = 0.3
            powerPricePos = np.random.uniform(low=100, high=500)
            powerPriceNeg = np.random.uniform(low=100, high=500)
            energyPricePos = np.random.uniform(low=0, high=50)
            energyPriceNeg = np.random.uniform(low=0, high=50)
            orderbook.update({'neg_%s' % i: {'quantity': np.round(states[1] * a, 0), 'powerPrice': powerPriceNeg,
                                             'energyPrice': energyPriceNeg, 'typ': 'neg', 'slot': i, 'name': self.name}})
            orderbook.update({'pos_%s' % i: {'quantity': np.round(states[0] * a, 0), 'powerPrice': powerPricePos,
                                             'energyPrice': energyPricePos, 'typ': 'pos', 'slot': i, 'name': self.name}})

        self.ConnectionMongo.setBalancing(self.name, self.date, orderbook)

        logging.info('Planung Regelleistungsmarkt abgeschlossen')

    def optimize_dayAhead(self):
        """Einsatzplanung für den DayAhead-Markt"""
        orderbook = dict()                                                  # Oderbook für alle Gebote (Stunde 1-24)
        # Prognosen für den kommenden Tag
        weather = self.weatherForecast()                                    # Wetterdaten (dir,dif,temp,wind)
        price = self.priceForecast()                                        # Preisdaten (power,gas,nuc,coal,lignite)
        demand = self.demandForecast()                                      # Lastprognose
        # Verpflichtungen Regelleistung
        # pos, neg = self.ConnectionInflux.getBalancingPower(self.date, self.name)

        # Standardoptimierung
        self.portfolio.setPara(self.date, weather,  price, demand)
        self.portfolio.buildModel()
        power = np.asarray(self.portfolio.optimize(), np.float)             # Berechnung der Einspeiseleitung
        try:
            E = np.asarray([np.round(self.portfolio.m.getVarByName('E[%i]' % i).x, 2) for i in self.portfolio.t])
            F = np.asarray([np.round(self.portfolio.m.getVarByName('F[%i]' % i).x, 2) for i in self.portfolio.t])
        except Exception as e:
            E = np.zeros_like(self.portfolio.t)
            F = np.zeros_like(self.portfolio.t)

        costs = [(E[i] + F[i])/power[i] if power[i] else 0 for i in self.portfolio.t]

        self.minPrice = np.asarray(costs).reshape((-1,))

        # Speichern des Fahrplans bei aktuller Preisprognose
        json_body = []
        for key, value in self.portfolio.energySystems.items():
            time = self.date
            try:
                power = [self.portfolio.m.getVarByName('P' + '_%s[%i]' % (key, i)).x for i in self.portfolio.t]
            except Exception as e:
                power = np.zeros_like(self.portfolio.t)
            volume = np.zeros_like(power)
            if value['typ'] == 'storage':
                try:
                    volume = [self.portfolio.m.getVarByName('V' + '_%s[%i]' % (key, i)).x for i in self.portfolio.t]
                except Exception as e:
                    volume = np.zeros_like(self.portfolio.t)
            for i in self.portfolio.t:
                json_body.append(
                    {
                        "measurement": 'Areas',
                        "tags": dict(plant=value['typ'], asset=key, agent=self.name, area=self.plz,
                                     timestamp='optimize_dayAhead', typ='PWP', fuel=value['fuel']),
                        "time": time.isoformat() + 'Z',
                        "fields": dict(Power=power[i], Volume=volume[i], PriceFrcst=price['power'][i])
                    }
                )
                time = time + pd.DateOffset(hours=self.portfolio.dt)
        self.ConnectionInflux.saveData(json_body)

        # Optimierung Maximale leistung
        self.portfolio.setPara(self.date, weather, price, demand)
        self.portfolio.buildModel(max_=True)
        powerMax = np.asarray(self.portfolio.optimize(), np.float)
        # Speichern der maximal möglichen Leistung
        json_body = []
        for key, value in self.portfolio.energySystems.items():
            time = self.date
            try:
                power = [self.portfolio.m.getVarByName('P' + '_%s[%i]' % (key, i)).x for i in self.portfolio.t]
            except Exception as e:
                power = np.zeros_like(self.portfolio.t)
            volume = np.zeros_like(power)
            if value['typ'] == 'storage':
                try:
                    volume = [self.portfolio.m.getVarByName('V' + '_%s[%i]' % (key, i)).x for i in self.portfolio.t]
                except Exception as e:
                    volume = np.zeros_like(self.portfolio.t)
            for i in self.portfolio.t:
                json_body.append(
                    {
                        "measurement": 'Areas',
                        "tags": dict(plant=value['typ'], asset=key, agent=self.name, area=self.plz,
                                     timestamp='optimize_dayAhead', typ='PWP', fuel=value['fuel']),
                        "time": time.isoformat() + 'Z',
                        "fields": dict(PowerMax=power[i], Volume=volume[i])
                    }
                )
                time = time + pd.DateOffset(hours=self.portfolio.dt)
        self.ConnectionInflux.saveData(json_body)

        try:
            E = np.asarray([np.round(self.portfolio.m.getVarByName('E[%i]' % i).x, 2) for i in self.portfolio.t])
            F = np.asarray([np.round(self.portfolio.m.getVarByName('F[%i]' % i).x, 2) for i in self.portfolio.t])
        except Exception as e:
            E = np.zeros_like(self.portfolio.t)
            F = np.zeros_like(self.portfolio.t)
        powerMax[powerMax <= 0] = self.portfolio.capacities['fossil']
        priceMax = ((E+F))/powerMax
        powerMax = powerMax - power

        # Aufbau der linearen Gebotskurven
        slopes = np.random.randint(10, 80, 24)
        wnd = np.asarray(weather['wind']).reshape((-1, 1))                  # Wind [m/s]
        rad = np.asarray(weather['dir']).reshape((-1, 1))                   # Dirkete Strahlung [W/m²]
        tmp = np.asarray(weather['temp']).reshape((-1, 1))                  # Temperatur [°C]
        dem = np.asarray(demand).reshape((-1, 1))                           # Lastprognose [MW]
        prc = np.asarray(price['power']).reshape((-1, 1))                   # MCP Porgnose
        if self.qLearn.fitted and (self.espilion > np.random.uniform(0,1)):
            # Wenn ein Modell vorliegt und keine neuen Möglichkeiten ausprobiert werden sollen
            slopes = self.qLearn.getAction(wnd, rad, tmp, dem, prc)

        self.actions = slopes                                               # abschpeichern der Ergebnisse

        var = np.sqrt(np.var(self.forecasts['price'].y) * self.forecasts['price'].factor)

        self.maxPrice = prc.reshape((-1,)) + max(3*var,2)

        delta = self.maxPrice - self.minPrice
        slopes = (delta/100) * np.tan((slopes+10)/180*np.pi)   # Preissteigung pro weitere MW

        # Füge für jede Stunde die entsprechenden Gebote hinzu
        for i in range(self.portfolio.T):
            # biete immer den minimalen Preis, aber nie mehr als den maximalen Preis
            quantity = [-1*(2/100 * power[i]) for _ in range(2, 102, 2)]
            price = [float(min(slopes[i] * p + self.minPrice[i], self.maxPrice[i])) for p in range(2, 102, 2)]
            if powerMax[i] > 0:
                quantity.append(-1 * powerMax[i])
                price.append(max(priceMax[i], (self.maxPrice[i] + 5)))
            orderbook.update({'h_%s' % i: {'quantity': quantity, 'price': price, 'hour': i, 'name': self.name}})

        self.ConnectionMongo.setDayAhead(name=self.name, date=self.date, orders=orderbook)

        logging.info('Planung DayAhead-Markt abgeschlossen')

    def post_dayAhead(self):
        """Reaktion auf  die DayAhead-Ergebnisse"""

        # Speichern der Daten und Aktionen, um aus diesen zu lernen
        self.qLearn.collectData(self.date, self.actions.reshape((24, 1)))
        # Abfrage der DayAhead Ergebnisse
        ask = self.ConnectionInflux.getDayAheadAsk(self.date, self.name)
        bid = self.ConnectionInflux.getDayAheadBid(self.date, self.name)
        price = self.ConnectionInflux.getDayAheadPrice(self.date)
        profit = np.asarray([(ask[i]-bid[i])*price[i] for i in range(24)])

        # Minimiere Differenz zu den bezuschlagten Geboten
        self.portfolio.buildModel(response=ask-bid)
        power = self.portfolio.fixPlaning()

        delta = power - (ask-bid)

        try:
            E = np.asarray([np.round(self.portfolio.m.getVarByName('E[%i]' % i).x, 2) for i in self.portfolio.t])
            F = np.asarray([np.round(self.portfolio.m.getVarByName('F[%i]' % i).x, 2) for i in self.portfolio.t])
        except Exception as e:
            E = np.zeros_like(self.portfolio.t)
            F = np.zeros_like(self.portfolio.t)
        costs = E+F
        profit = profit.reshape((24,)) - costs.reshape((24,))
        # Falls ein Modell des Energiesystems vorliegt, passe die Gewinnerwartung entsprechend der Lernrate an
        if self.qLearn.fitted:
            states = self.qLearn.getStates(self.date)
            for i in self.portfolio.t:
                oldValue = self.qLearn.qus[states[i], int(self.actions[i]-10)]
                self.qLearn.qus[states[i], int(self.actions[i]-10)] = oldValue + self.lr * (profit[i] - np.abs(delta[i]) * 1000 - oldValue)

        # Abspeichern der Ergebnisse
        json_body = []
        for key, value in self.portfolio.energySystems.items():
            time = self.date
            try:
                power = [self.portfolio.m.getVarByName('P' + '_%s[%i]' % (key, i)).x for i in self.portfolio.t]
            except Exception as e:
                power = np.zeros_like(self.portfolio.t)
            volume = np.zeros_like(power)
            if value['typ'] == 'storage':
                try:
                    volume = [self.portfolio.m.getVarByName('V' + '_%s[%i]' % (key, i)).x for i in self.portfolio.t]
                except Exception as e:
                    volume = np.zeros_like(self.portfolio.t)
            for i in self.portfolio.t:
                json_body.append(
                    {
                        "measurement": 'Areas',
                        "tags": dict(plant=value['typ'], asset=key, agent=self.name, area=self.plz,
                                     timestamp='post_dayAhead', typ='PWP', fuel=value['fuel']),
                        "time": time.isoformat() + 'Z',
                        "fields": dict(Power=power[i], Volume=volume[i])
                    }
                )
                time = time + pd.DateOffset(hours=self.portfolio.dt)
        self.ConnectionInflux.saveData(json_body)

        logging.info('DayAhead Ergebnisse erhalten')

    def optimize_actual(self):
        """Abruf Prognoseabweichung und Übermittlung der Fahrplanabweichung"""
        ask = self.ConnectionInflux.getDayAheadAsk(self.date, self.name)
        bid = self.ConnectionInflux.getDayAheadBid(self.date, self.name)
        schedule = self.ConnectionInflux.getPowerScheduling(self.date, self.name, 'post_dayAhead')

        # Berechnung der Abweichung, die zum Abruf von Regelenergie führt
        difference = schedule-(ask-bid)
        # Aufbau der "Gebote" (Abweichungen zum gemeldeten Fahrplan)
        orderbook = dict()
        for i in range(self.portfolio.T):
            orderbook.update({'h_%s' % i: {'quantity': difference[i], 'hour': i, 'name': self.name}})
        self.ConnectionMongo.setActuals(name=self.name, date=self.date, orders=orderbook)

        # Abspeichern der Ergebnisse
        json_body = []
        for key, value in self.portfolio.energySystems.items():
            time = self.date
            try:
                power = [self.portfolio.m.getVarByName('P' + '_%s[%i]' % (key, i)).x for i in self.portfolio.t]
            except Exception as e:
                power = np.zeros_like(self.portfolio.t)
            volume = np.zeros_like(power)
            if value['typ'] == 'storage':
                try:
                    volume = [self.portfolio.m.getVarByName('V' + '_%s[%i]' % (key, i)).x for i in self.portfolio.t]
                except Exception as e:
                    volume = np.zeros_like(self.portfolio.t)
            for i in self.portfolio.t:
                json_body.append(
                    {
                        "measurement": 'Areas',
                        "tags": dict(plant=value['typ'], asset=key, agent=self.name, area=self.plz,
                                     timestamp='optimize_actual', typ='PWP', fuel=value['fuel']),
                        "time": time.isoformat() + 'Z',
                        "fields": dict(Power=power[i], Volume=volume[i])
                    }
                )
                time = time + pd.DateOffset(hours=self.portfolio.dt)
        self.ConnectionInflux.saveData(json_body)

    logging.info('Aktuellen Fahrplan erstellt')

    def post_actual(self):
        """Abschlussplanung des Tages"""
        #pos, neg = self.ConnectionInflux.getBalancingEnergy(self.date,self.name)
        #self.portfolio.setPara(self.date, self.weatherForecast(), self.priceForecast(), self.demandForecast(),
        #                       np.zeros(self.portfolio.T), np.zeros(self.portfolio.T))
        #schedule = self.ConnectionInflux.getPowerScheduling(self.date, self.name, 'optimize_actual')
        #self.portfolio.buildModel(response=schedule + pos - neg)
        #power = self.portfolio.fixPlaning()
        #power = schedule
        # Abspeichern der Ergebnisse
        json_body = []
        for key, value in self.portfolio.energySystems.items():
            time = self.date
            try:
                power = [self.portfolio.m.getVarByName('P' + '_%s[%i]' % (key, i)).x for i in self.portfolio.t]
            except Exception as e:
                power = np.zeros_like(self.portfolio.t)
            volume = np.zeros_like(power)
            if value['typ'] == 'storage':
                try:
                    volume = [self.portfolio.m.getVarByName('V' + '_%s[%i]' % (key, i)).x for i in self.portfolio.t]
                except Exception as e:
                    volume = np.zeros_like(self.portfolio.t)
            for i in self.portfolio.t:
                json_body.append(
                    {
                        "measurement": 'Areas',
                        "tags": dict(plant=value['typ'], asset=key, agent=self.name, area=self.plz,
                                     timestamp='post_actual', typ='PWP', fuel=value['fuel']),
                        "time": time.isoformat() + 'Z',
                        "fields": dict(Power=power[i], Volume=volume[i])
                    }
                )
                time = time + pd.DateOffset(hours=self.portfolio.dt)
        self.ConnectionInflux.saveData(json_body)

        # Planung für den nächsten Tag
        # Anpassung der Prognosemethoden für den Verbrauch und die Preise
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
        self.espilion = max(0.9*self.espilion, 0.2)                     # Epsilion * 0.9 (mit steigender Simulationdauer
                                                                        # sind viele Bereiche schon bekannt

        logging.info('Tag %s abgeschlossen' %self.date)
        print('Agent %s %s done' % (self.name, self.date.date()))

if __name__ == "__main__":

    args = parse_args()
    agent = pwpAgent(date='2019-01-01', plz=args.plz, mongo=args.mongo, influx=args.influx,
                     market=args.market, dbName=args.dbName)
    agent.ConnectionMongo.login(agent.name, True)
    try:
        agent.run_agent()
    except Exception as e:
        logging.error('Fehler in run_agent: %s' %e)
    finally:
        agent.ConnectionInflux.influx.close()
        agent.ConnectionMongo.logout(agent.name)
        agent.ConnectionMongo.mongo.close()
        if agent.receive.is_open:
            agent.receive.close()
            agent.connection.close()
        exit()
