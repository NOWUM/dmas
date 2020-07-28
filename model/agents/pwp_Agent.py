import os
os.chdir(os.path.dirname(os.path.dirname(__file__)))
from aggregation.pwp_Port import pwpPort
from agents.basic_Agent import agent as basicAgent
from apps.qLearn_DayAhead import qLeran as daLearning
import argparse
import pandas as pd
import numpy as np


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--plz', type=int, required=False, default=50, help='PLZ-Agent')
    return parser.parse_args()

class pwpAgent(basicAgent):

    def __init__(self, date, plz):
        super().__init__(date=date, plz=plz, exchange='Market', typ='PWP')

        self.logger.info('Start des Agenten')

        # Aufbau des Portfolios mit den enstprechenden Kraftwerken und Speichern
        self.portfolio = pwpPort(typ='PWP', gurobi=True, T=48)                  # Verwendung von Gurobi

        # Einbindung der Kraftwerksdaten aus der MongoDB
        # storages = myMongo.getStorages()
        for key, value in self.ConnectionMongo.getPowerPlants().items():
            if value['maxPower'] > 1:
                self.portfolio.addToPortfolio(key, {key: value})
                self.portfolio.capacities['fossil'] += value['maxPower']        # Gesamte Kraftwerksleitung  in [MW]
        self.logger.info('Kraftwerke hinzugefügt')

        # Einbindung der Speicherdaten aus der MongoDB
        for key, value in self.ConnectionMongo.getStorages().items():
            self.portfolio.addToPortfolio(key, {key: value})

        self.logger.info('Speicher hinzugefügt')

        # Parameter für die Handelsstrategie am Day Ahead Markt
        self.maxPrice = np.zeros(24)                                                            # Maximalgebote
        self.minPrice = np.zeros(24)                                                            # Minimalgenbote
        self.actions = np.zeros(24)                                                             # Steigung der Gebotsgeraden für jede Stunde
        self.espilion = 0.7                                                                     # Faktor zum Abtasten der Möglichkeiten
        self.lr = 0.8                                                                           # Lernrate des Q-Learning-Einsatzes
        self.qLearn = daLearning(self.ConnectionInflux, init=np.random.randint(5, 10 + 1))      # Lernalgorithmus im x Tage Rythmus
        self.qLearn.qus *= 0.5 * self.portfolio.capacities['fossil']
        self.risk = np.random.choice([-5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5])

        if len(self.portfolio.energySystems) == 0:
            self.logger.info('Keine Kraftwerke im PLZ-Gebiet vorhanden')
            exit()

        self.logger.info('Parameter der Handelsstrategie festgelegt')

        self.logger.info('Aufbau des Agenten abgeschlossen')

    def optimize_balancing(self):
        """Einsatzplanung für den Regelleistungsmarkt"""
        self.logger.info('Planung Regelleistungsmarkt abgeschlossen')

    def optimize_dayAhead(self):
        """Einsatzplanung für den DayAhead-Markt"""
        orderbook = dict()                                                  # Oderbook für alle Gebote (Stunde 1-24)
        json_body = []                                                      # Liste zur Speicherung der Ergebnisse in der InfluxDB

        # Prognosen für den kommenden Tag
        weather = self.weatherForecast(self.date, 2)                        # Wetterdaten (dir,dif,temp,wind)
        price = self.priceForecast(self.date, 2)                            # Preisdaten (power,gas,nuc,coal,lignite)
        demand = self.demandForecast(self.date, 2)                          # Lastprognose

        # pos, neg = self.ConnectionInflux.getBalancingPower(self.date, self.name)  # Verpflichtungen Regelleistung

        # Standardoptimierung
        self.portfolio.setPara(self.date, weather,  price, demand)
        self.portfolio.buildModel()
        power_dayAhead = np.asarray(self.portfolio.optimize(), np.float)
        emission = self.portfolio.emisson
        fuel = self.portfolio.fuel
        costs = [(emission[i] + fuel[i])/power_dayAhead[i] if power_dayAhead[i] != 0 else 0 for i in self.portfolio.t]

        # verdiene mindestens die variablen Kosten
        self.minPrice = np.asarray(costs[:24]).reshape((-1,))                        # Minimalpreis      [€/MWh]

        # Energiesysteminformation
        for key, value in self.portfolio.energySystems.items():
            time = self.date
            power = value['model'].power
            volume = value['model'].volume
            for i in self.portfolio.t:
                json_body.append(
                    {
                        "measurement": 'Areas',
                        "tags": dict(typ='PWP',                             # Typ konventionelle Kraftwerke
                                     fuel=value['fuel'],                    # Brennstoff/ Energieträger (Braunkohle, Steinkohle, Erdgas, Kernkraft)
                                     asset=key,                             # eindeutiger Name des Energiesystems
                                     agent=self.name,                       # Name des Agenten
                                     area=self.plz,                         # Plz Gebiet
                                     timestamp='optimize_dayAhead'),        # Zeitstempel der Tagesplanung
                        "time": time.isoformat() + 'Z',
                        "fields": dict(power=power[i],                      # Gesamtleistung des Energiesystems [MW]
                                       volume=volume[i])                    # Speichervolumen                   [MWh]
                    }
                )
                time = time + pd.DateOffset(hours=self.portfolio.dt)

        # Berechne maximal verfügbare Leistung
        self.portfolio.buildModel(max_=True)
        power_max = np.asarray(self.portfolio.optimize(), np.float)
        power_max[power_max <= 0] = self.portfolio.capacities['fossil']
        # print(self.portfolio.capacities['fossil'])
        priceMax = (self.portfolio.emisson + self.portfolio.fuel) / power_max
        power_max = power_max - power_dayAhead

        # Portfolioinformation
        time = self.date
        for i in self.portfolio.t:
            json_body.append(
                {
                    "measurement": 'Areas',
                    "tags": dict(typ='PWP',                                                 # Typ konventionelle Kraftwerke
                                 agent=self.name,                                           # Name des Agenten
                                 area=self.plz,                                             # Plz Gebiet
                                 timestamp='optimize_dayAhead'),                            # Zeitstempel der Tagesplanung
                    "time": time.isoformat() + 'Z',
                    "fields": dict(powerMax=power_max[i] + power_dayAhead[i],               # maximal mögliche Leistung     [MW]
                                   powerTotal=power_dayAhead[i],                            # gesamte geplante Leistung     [MW]
                                   emissionCost=emission[i],                                # Kosten aus CO2                [€]
                                   fuelCost=fuel[i],                                        # Kosten aus Brennstoff         [€]
                                   priceForcast=price['power'][i],                          # Day Ahead Preisprognose       [€/MWh]
                                   powerLignite=self.portfolio.generation['lignite'][i],    # gesamt Braunkohle             [MW]
                                   powerCoal=self.portfolio.generation['coal'][i],          # gesamt Steinkohle             [MW]
                                   powerGas=self.portfolio.generation['gas'][i],            # gesamt Erdgas                 [MW]
                                   powerNuc=self.portfolio.generation['nuc'][i],            # gesamt Kernkraft              [MW]
                                   powerStorage=self.portfolio.generation['water'][i])
                }
            )
            time = time + pd.DateOffset(hours=self.portfolio.dt)

        self.ConnectionInflux.saveData(json_body)

        # Aufbau der linearen Gebotskurven
        actions = np.random.randint(1, 8, 24) * 10
        prc = np.asarray(price['power'][:24]).reshape((-1, 1))                               # MCP Porgnose      [€/MWh]

        # Wenn ein Modell vorliegt und keine neuen Möglichkeiten ausprobiert werden sollen
        if self.qLearn.fitted:
            wnd = np.asarray(weather['wind'][:24]).reshape((-1, 1))     # Wind              [m/s]
            rad = np.asarray(weather['dir'][:24]).reshape((-1, 1))      # Dirkete Strahlung [W/m²]
            tmp = np.asarray(weather['temp'][:24]).reshape((-1, 1))     # Temperatur        [°C]
            dem = np.asarray(demand[:24]).reshape((-1, 1))              # Lastprognose      [MW]
            actionsBest = self.qLearn.getAction(wnd, rad, tmp, dem, prc)

            for i in range(24):
                if self.espilion < np.random.uniform(0, 1):
                    actions[i] = actionsBest[i]

        self.actions = np.asarray(actions).reshape(24,)                                                              # abschpeichern der Aktionen

        # Berechnung der Prognosegüte
        var = np.sqrt(np.var(self.forecasts['price'].mcp, axis=0) * self.forecasts['price'].factor)
        var = np.nan_to_num(var)
        self.maxPrice = prc.reshape((-1,)) + np.asarray([max(self.risk*v, 1) for v in var])                       # Maximalpreis      [€/MWh]

        # Füge für jede Stunde die entsprechenden Gebote hinzu
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

            quantity = [float(-1 * (2 / 100 * (power_dayAhead[i]-self.portfolio.generation['water'][i]))) for _ in range(2, 102, 2)]

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
        self.logger.info('Planung DayAhead-Markt abgeschlossen')

    def post_dayAhead(self):
        """Reaktion auf  die DayAhead-Ergebnisse"""
        json_body = []                                                      # Liste zur Speicherung der Ergebnisse in der InfluxDB

        # Speichern der Daten und Aktionen, um aus diesen zu lernen
        self.qLearn.collectData(self.date, self.actions.reshape((24, 1)))

        # Abfrage der DayAhead Ergebnisse
        ask = self.ConnectionInflux.getDayAheadAsk(self.date, self.name, days=2)                            # Angebotene Menge [MWh]
        bid = self.ConnectionInflux.getDayAheadBid(self.date, self.name, days=2)                            # Nachgefragte Menge [MWh]
        price = self.ConnectionInflux.getDayAheadPrice(self.date, days=2)                                   # MCP [€/MWh]

        profit = np.asarray([float((ask[i] - bid[i]) * price[i]) for i in self.portfolio.t])                # erzielte Erlöse

        # Minimiere Differenz zu den bezuschlagten Geboten
        # Prognosen für den kommenden Tag
        weather = self.weatherForecast(self.date, 2)                        # Wetterdaten (dir,dif,temp,wind)
        price = self.priceForecast(self.date, 2)                            # Preisdaten (power,gas,nuc,coal,lignite)
        demand = self.demandForecast(self.date, 2)                          # Lastprognose

        # Standardoptimierung
        self.portfolio.setPara(self.date, weather,  price, demand)

        self.portfolio.buildModel(response=ask-bid)
        power_dayAhead = self.portfolio.fixPlaning()
        costs = self.portfolio.emisson + self.portfolio.fuel
        profit = profit.reshape((-1,)) - costs.reshape((-1,))
        # Falls ein Modell des Energiesystems vorliegt, passe die Gewinnerwartung entsprechend der Lernrate an
        if self.qLearn.fitted:
            states = self.qLearn.getStates(self.date)
            for i in range(24):
                oldValue = self.qLearn.qus[states[i], int((self.actions[i]-10)/10)]
                self.qLearn.qus[states[i], int((self.actions[i]-10)/10)] = oldValue + self.lr * (profit[i] - oldValue) # np.abs(delta[i]) * 1000
        else:
            states = [-1 for i in self.portfolio.t]

        # Energiesysteminformation
        for key, value in self.portfolio.energySystems.items():
            time = self.date
            power = value['model'].power
            volume = value['model'].volume
            for i in self.portfolio.t:
                json_body.append(
                    {
                        "measurement": 'Areas',
                        "tags": dict(typ='PWP',                             # Typ konventionelle Kraftwerke
                                     fuel=value['fuel'],                    # Brennstoff/ Energieträger (Braunkohle, Steinkohle, Erdgas, Kernkraft)
                                     asset=key,                             # eindeutiger Name des Energiesystems
                                     agent=self.name,                       # Name des Agenten
                                     area=self.plz,                         # Plz Gebiet
                                     timestamp='post_dayAhead'),            # Zeitstempel der Tagesplanung
                        "time": time.isoformat() + 'Z',
                        "fields": dict(power=power[i],                      # Gesamtleistung des Energiesystems [MW]
                                       volume=volume[i])                    # Speichervolumen                   [MWh]
                    }
                )
                time = time + pd.DateOffset(hours=self.portfolio.dt)

        # Portfolioinformation
        time = self.date
        index = 0
        for i in self.portfolio.t:
            json_body.append(
                {
                    "measurement": 'Areas',
                    "tags": dict(typ='PWP',                                                 # Typ konventionelle Kraftwerke
                                 agent=self.name,                                           # Name des Agenten
                                 area=self.plz,                                             # Plz Gebiet
                                 timestamp='post_dayAhead'),                                # Zeitstempel der Tagesplanung
                    "time": time.isoformat() + 'Z',
                    "fields": dict(powerTotal=power_dayAhead[i],                            # gesamte geplante Leistung     [MW]
                                   emissionCost=self.portfolio.emisson[i],                  # Kosten aus CO2                [€]
                                   fuelCost=self.portfolio.fuel[i],                         # Kosten aus Brennstoff         [€]
                                   powerLignite=self.portfolio.generation['lignite'][i],    # gesamt Braunkohle             [MW]
                                   powerCoal=self.portfolio.generation['coal'][i],          # gesamt Steinkohle             [MW]
                                   powerGas=self.portfolio.generation['gas'][i],            # gesamt Erdgas                 [MW]
                                   powerNuc=self.portfolio.generation['nuc'][i],            # gesamt Kernkraft              [MW]
                                   powerStorage=self.portfolio.generation['water'][i],
                                   profit=profit[i],                                        # erzielte Erlöse               [€]
                                   state=int(states[index]),
                                   action=int((self.actions[index] - 10) / 10))
                }
            )
            index += 1
            index = min(index, 23)
            time = time + pd.DateOffset(hours=self.portfolio.dt)

        self.ConnectionInflux.saveData(json_body)

        self.logger.info('DayAhead Ergebnisse erhalten')

    def optimize_actual(self):
        """Abruf Prognoseabweichung und Übermittlung der Fahrplanabweichung"""
        # TODO: Überarbeitung, wenn Regelleistung
        self.logger.info('Aktuellen Fahrplan erstellt')

    def post_actual(self):
        """Abschlussplanung des Tages"""
        # TODO: Überarbeitung, wenn Regelleistung
        # Planung für den nächsten Tag
        # Anpassung der Prognosemethoden für den Verbrauch und die Preise
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

        self.logger.info('Tag %s abgeschlossen' %self.date)
        print('Agent %s %s done' % (self.name, self.date.date()))

if __name__ == "__main__":

    args = parse_args()
    agent = pwpAgent(date='2019-01-01', plz=args.plz)
    agent.ConnectionMongo.login(agent.name, True)
    try:
        agent.run_agent()
    except Exception as e:
        print(e)
    finally:
        agent.ConnectionInflux.influx.close()
        agent.ConnectionMongo.logout(agent.name)
        agent.ConnectionMongo.mongo.close()
        if agent.receive.is_open:
            agent.receive.close()
            agent.connection.close()
        exit()
