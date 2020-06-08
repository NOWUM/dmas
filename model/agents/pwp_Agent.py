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
    parser.add_argument('--plz', type=int, required=False, default=50, help='PLZ-Agent')
    parser.add_argument('--mongo', type=str, required=False, default='149.201.88.150', help='IP MongoDB')
    parser.add_argument('--influx', type=str, required=False, default='149.201.88.150', help='IP InfluxDB')
    parser.add_argument('--market', type=str, required=False, default='149.201.88.150', help='IP Market')
    parser.add_argument('--dbName', type=str, required=False, default='MAS_XXXX', help='Name der Datenbank')
    return parser.parse_args()

class pwpAgent(basicAgent):

    def __init__(self, date, plz, mongo='149.201.88.150', influx='149.201.88.150', market='149.201.88.150', dbName='MAS_XXXX'):
        super().__init__(date=date, plz=plz, mongo=mongo, influx=influx, market=market, exchange='Market', typ='PWP', dbName=dbName)

        self.logger.info('Start des Agenten')

        # Aufbau des Portfolios mit den enstprechenden Kraftwerken und Speichern
        self.portfolio = pwpPort(typ='PWP', gurobi=True, T=48)                  # Verwendung von Gurobi

        # Einbindung der Kraftwerksdaten aus der MongoDB
        totalPower = 0
        powerPlants = self.ConnectionMongo.getPowerPlants(plz)
        for key, value in powerPlants.items():
            if value['maxPower'] > 1:
                self.portfolio.addToPortfolio(key, {key: value})
                totalPower += value['maxPower']                                 # Gesamte Kraftwerksleitung  in [MW]
        self.portfolio.capacities['fossil'] = totalPower

        self.logger.info('Kraftwerke hinzugefügt')

        # Einbindung der Speicherdaten aus der MongoDB
        storages = self.ConnectionMongo.getStorages(plz)
        for key, value in storages.items():
            self.portfolio.addToPortfolio(key, {key: value})

        self.logger.info('Speicher hinzugefügt')

        # Parameter für die Handelsstrategie am Day Ahead Markt
        self.maxPrice = np.zeros(24)                                                             # Maximalgebote
        self.minPrice = np.zeros(24)                                                            # Minimalgenbote
        self.actions = np.zeros(24)                                                             # Steigung der Gebotsgeraden für jede Stunde
        self.espilion = 0.8                                                                     # Faktor zum Abtasten der Möglichkeiten
        self.lr = 0.8                                                                           # Lernrate des Q-Learning-Einsatzes
        self.qLearn = daLearning(self.ConnectionInflux, init=np.random.randint(5, 10 + 1))      # Lernalgorithmus im x Tage Rythmus
        self.qLearn.qus[:, 0] = self.qLearn.qus[:, 0] * self.portfolio.capacities['fossil']
        self.risk = np.random.choice([-2, -1, 0, 1, 2])

        if len(self.portfolio.energySystems) == 0 or plz==79:           # TODO: Check PLZ Gebiet 79
            self.logger.info('Keine Kraftwerke im PLZ-Gebiet vorhanden')
            exit()

        self.logger.info('Parameter der Handelsstrategie festgelegt')

        self.logger.info('Aufbau des Agenten abgeschlossen')

    def optimize_balancing(self):
        # Prognosen für den kommenden Tag
        weather = self.weatherForecast(self.date, 2)                        # Wetterdaten (dir,dif,temp,wind)
        price = self.priceForecast(self.date, 2)                            # Preisdaten (power,gas,nuc,coal,lignite)
        demand = self.demandForecast(self.date, 2)                          # Lastprognose

        price['power'] = 50*np.ones_like(price['power'])
        tau = 45
        maxPrice = max([np.exp(-k / tau) for k in range(5 * tau)])

        best = 0

        for i in np.arange(0, self.portfolio.capacities['fossil']/4 + self.portfolio.capacities['fossil']/20, self.portfolio.capacities['fossil']/20):

            # Standardoptimierung
            self.portfolio.setPara(self.date, weather,  price, demand, posBalPower=i, negBalPower=i)
            self.portfolio.buildModel()
            _ = np.asarray(self.portfolio.optimize(), np.float)

            emission = self.portfolio.emisson[:24]
            fuel = self.portfolio.fuel[:24]
            cash = self.portfolio.prices['power'][:24] * self.portfolio.power[:24]
            sum_ = sum(cash) - sum(emission+fuel)
            sum__ = 24*maxPrice * i
            total = np.round(sum_ + sum__,1)

            if i == 0:
                best = total
                power = 0
            elif best < total:
                best = total
                power = i

            print('%s: %s' % (i, total))

        print(power)
        print(best)

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

        powerFuels = dict(lignite=np.zeros_like(self.portfolio.t, dtype=float),              # gesamte Erzeugung aus Braunkohle
                          coal=np.zeros_like(self.portfolio.t, dtype=float),                 # gesamte Erzeugung aus Steinkohle
                          gas=np.zeros_like(self.portfolio.t, dtype=float),                  # gesamte Erzeugung aus Erdgas
                          nuc=np.zeros_like(self.portfolio.t, dtype=float),                  # gesamte Erzeugung aus Kernkraft
                          water=np.zeros_like(self.portfolio.t, dtype=float))

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
                powerFuels[value['fuel']][i] += power[i]                    # Summe der Erzeugungsleistung pro Energieträger
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
                    "tags": dict(typ='PWP',                                 # Typ konventionelle Kraftwerke
                                 agent=self.name,                           # Name des Agenten
                                 area=self.plz,                             # Plz Gebiet
                                 timestamp='optimize_dayAhead'),            # Zeitstempel der Tagesplanung
                    "time": time.isoformat() + 'Z',
                    "fields": dict(powerMax=power_max[i] + power_dayAhead[i],   # maximal mögliche Leistung     [MW]
                                   powerTotal=power_dayAhead[i],                # gesamte geplante Leistung     [MW]
                                   emissionCost=emission[i],                    # Kosten aus CO2                [€]
                                   fuelCost=fuel[i],                            # Kosten aus Brennstoff         [€]
                                   priceForcast=price['power'][i],              # Day Ahead Preisprognose       [€/MWh]
                                   powerLignite=powerFuels['lignite'][i],       # gesamt Braunkohle             [MW]
                                   powerCoal=powerFuels['coal'][i],             # gesamt Steinkohle             [MW]
                                   powerGas=powerFuels['gas'][i],               # gesamt Erdgas                 [MW]
                                   powerNuc=powerFuels['nuc'][i],               # gesamt Kernkraft              [MW]
                                   powerStorage=powerFuels['water'][i])
                }
            )
            time = time + pd.DateOffset(hours=self.portfolio.dt)

        self.ConnectionInflux.saveData(json_body)

        # Aufbau der linearen Gebotskurven
        actions = np.random.randint(1, 8, 24) * 10
        prc = np.asarray(price['power'][:24]).reshape((-1, 1))                               # MCP Porgnose      [€/MWh]

        # Wenn ein Modell vorliegt und keine neuen Möglichkeiten ausprobiert werden sollen
        if self.qLearn.fitted and (self.espilion > np.random.uniform(0, 1)):
            wnd = np.asarray(weather['wind'][:24]).reshape((-1, 1))                          # Wind              [m/s]
            rad = np.asarray(weather['dir'][:24]).reshape((-1, 1))                           # Dirkete Strahlung [W/m²]
            tmp = np.asarray(weather['temp'][:24]).reshape((-1, 1))                          # Temperatur        [°C]
            dem = np.asarray(demand[:24]).reshape((-1, 1))                                   # Lastprognose      [MW]
            actions = self.qLearn.getAction(wnd, rad, tmp, dem, prc)
        self.actions = actions                                                               # abschpeichern der Aktionen

        # Berechnung der Prognosegüte
        var = np.sqrt(np.var(self.forecasts['price'].y) * self.forecasts['price'].factor)

        self.maxPrice = prc.reshape((-1,)) + max(self.risk*var, 1)                       # Maximalpreis      [€/MWh]

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

            quantity = [float(-1 * (2 / 100 * (power_dayAhead[i]-powerFuels['water'][i]))) for _ in range(2, 102, 2)]

            mcp = self.maxPrice[i]
            cVar = self.minPrice[i]

            if (cVar > mcp) and power_dayAhead[i] > 0:
                if delta > 0:
                    lb = mcp - min(3*var/power_dayAhead[i], delta/power_dayAhead[i])
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

            if powerFuels['water'][i] > 0:
                for p in range(2, 102, 2):
                    lb = minSell
                    slope = (mcp - lb) / 100 * np.tan(45/180 * np.pi)
                    price.append(float(min(slope * p + lb, mcp)))
                    quantity.append(float(-1 * (2 / 100 * powerFuels['water'][i])))
            if powerFuels['water'][i] < 0:
                for p in range(2, 102, 2):
                    lb = maxBuy
                    slope = (mcp - lb) / 100 * np.tan(45/180 * np.pi)
                    price.append(float(min(slope * p + lb, mcp)))
                    quantity.append(float(-1 * (2 / 100 * powerFuels['water'][i])))

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

        powerFuels = dict(lignite=np.zeros_like(self.portfolio.t, dtype=float),              # gesamte Erzeugung aus Braunkohle
                          coal=np.zeros_like(self.portfolio.t, dtype=float),                 # gesamte Erzeugung aus Steinkohle
                          gas=np.zeros_like(self.portfolio.t, dtype=float),                  # gesamte Erzeugung aus Erdgas
                          nuc=np.zeros_like(self.portfolio.t, dtype=float),                  # gesamte Erzeugung aus Kernkraft
                          water=np.zeros_like(self.portfolio.t, dtype=float))

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
                powerFuels[value['fuel']][i] += power[i]                    # Summe der Erzeugungsleistung pro Energieträger
                time = time + pd.DateOffset(hours=self.portfolio.dt)

        # Portfolioinformation
        time = self.date
        for i in self.portfolio.t:
            json_body.append(
                {
                    "measurement": 'Areas',
                    "tags": dict(typ='PWP',                                 # Typ konventionelle Kraftwerke
                                 agent=self.name,                           # Name des Agenten
                                 area=self.plz,                             # Plz Gebiet
                                 timestamp='post_dayAhead'),                # Zeitstempel der Tagesplanung
                    "time": time.isoformat() + 'Z',
                    "fields": dict(powerTotal=power_dayAhead[i],            # gesamte geplante Leistung     [MW]
                                   emissionCost=self.portfolio.emisson[i],  # Kosten aus CO2                [€]
                                   fuelCost=self.portfolio.fuel[i],         # Kosten aus Brennstoff         [€]
                                   powerLignite=powerFuels['lignite'][i],   # gesamt Braunkohle             [MW]
                                   powerCoal=powerFuels['coal'][i],         # gesamt Steinkohle             [MW]
                                   powerGas=powerFuels['gas'][i],           # gesamt Erdgas                 [MW]
                                   powerNuc=powerFuels['nuc'][i],           # gesamt Kernkraft              [MW]
                                   powerStorage=powerFuels['water'][i],
                                   profit=profit[i])                        # erzielte Erlöse               [€]
                }
            )
            time = time + pd.DateOffset(hours=self.portfolio.dt)

        self.ConnectionInflux.saveData(json_body)

        self.logger.info('DayAhead Ergebnisse erhalten')

    def optimize_actual(self):
        """Abruf Prognoseabweichung und Übermittlung der Fahrplanabweichung"""
        # TODO: Überarbeitung, wenn Regelleistung
        json_body = []  # Liste zur Speicherung der Ergebnisse in der InfluxDB

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

        powerFuels = dict(lignite=np.zeros_like(self.portfolio.t), coal=np.zeros_like(self.portfolio.t), gas=np.zeros_like(self.portfolio.t),
                          nuc=np.zeros_like(self.portfolio.t), water=np.zeros_like(self.portfolio.t))
        try:
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
                            "tags": dict(typ='PWP', fuel=value['fuel'], asset=key, agent=self.name, area=self.plz, timestamp='optimize_actual'),
                            "time": time.isoformat() + 'Z',
                            "fields": dict(power=power[i], volume=volume[i])
                        }
                    )
                    powerFuels[value['fuel']][i] += power[i]
                    time = time + pd.DateOffset(hours=self.portfolio.dt)
        except Exception as e:
            print('Error: %s' % e)

        try:
            time = self.date
            for i in self.portfolio.t:
                json_body.append(
                    {
                        "measurement": 'Areas',
                        "tags": dict(typ='PWP', agent=self.name, area=self.plz, timestamp='optimize_actual'),
                        "time": time.isoformat() + 'Z',
                        "fields": dict(powerTotal=self.portfolio.power[i], emissionCost=self.portfolio.emisson[i],
                                       fuelCost=self.portfolio.fuel[i], powerLignite=powerFuels['lignite'][i],
                                       powerCoal=powerFuels['coal'][i], powerGas=powerFuels['gas'][i], powerNuc=powerFuels['nuc'][i])
                    }
                )
                time = time + pd.DateOffset(hours=self.portfolio.dt)
        except Exception as e:
            print('Error: %s' % e)

        self.ConnectionInflux.saveData(json_body)


        self.logger.info('Aktuellen Fahrplan erstellt')

    def post_actual(self):
        """Abschlussplanung des Tages"""
        # TODO: Überarbeitung, wenn Regelleistung
        # json_body = []  # Liste zur Speicherung der Ergebnisse in der InfluxDB
        #
        # powerFuels = dict(lignite=np.zeros_like(self.portfolio.t), coal=np.zeros_like(self.portfolio.t), gas=np.zeros_like(self.portfolio.t),
        #                   nuc=np.zeros_like(self.portfolio.t), water=np.zeros_like(self.portfolio.t))
        # try:
        #     for key, value in self.portfolio.energySystems.items():
        #         time = self.date
        #         power = [self.portfolio.m.getVarByName('P' + '_%s[%i]' % (key, i)).x for i in self.portfolio.t]
        #         volume = np.zeros_like(power)
        #         if value['typ'] == 'storage':
        #             volume = [self.portfolio.m.getVarByName('V' + '_%s[%i]' % (key, i)).x for i in self.portfolio.t]
        #         for i in self.portfolio.t:
        #             json_body.append(
        #                 {
        #                     "measurement": 'Areas',
        #                     "tags": dict(typ='PWP', fuel=value['fuel'], asset=key, agent=self.name, area=self.plz, timestamp='post_actual'),
        #                     "time": time.isoformat() + 'Z',
        #                     "fields": dict(power=power[i], volume=volume[i])
        #                 }
        #             )
        #             powerFuels[value['fuel']][i] += power[i]
        #             time = time + pd.DateOffset(hours=self.portfolio.dt)
        # except Exception as e:
        #     print('Error: %s' % e)
        #
        # try:
        #     time = self.date
        #     for i in self.portfolio.t:
        #         json_body.append(
        #             {
        #                 "measurement": 'Areas',
        #                 "tags": dict(typ='PWP', agent=self.name, area=self.plz, timestamp='post_actual'),
        #                 "time": time.isoformat() + 'Z',
        #                 "fields": dict(powerTotal=self.portfolio.power[i], emissionCost=self.portfolio.emisson[i],
        #                                fuelCost=self.portfolio.fuel[i], powerLignite=powerFuels['lignite'][i],
        #                                powerCoal=powerFuels['coal'][i], powerGas=powerFuels['gas'][i], powerNuc=powerFuels['nuc'][i])
        #             }
        #         )
        #         time = time + pd.DateOffset(hours=self.portfolio.dt)
        # except Exception as e:
        #     print('Error: %s' % e)
        #
        # self.ConnectionInflux.saveData(json_body)

        #pos, neg = self.ConnectionInflux.getBalancingEnergy(self.date,self.name)
        #self.portfolio.setPara(self.date, self.weatherForecast(), self.priceForecast(), self.demandForecast(),
        #                       np.zeros(self.portfolio.T), np.zeros(self.portfolio.T))
        #schedule = self.ConnectionInflux.getPowerScheduling(self.date, self.name, 'optimize_actual')
        #self.portfolio.buildModel(response=schedule + pos - neg)
        #power = self.portfolio.fixPlaning()
        #power = schedule

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

        self.logger.info('Tag %s abgeschlossen' %self.date)
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
