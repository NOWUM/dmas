import os
os.chdir(os.path.dirname(os.path.dirname(__file__)))
from aggregation.dem_Port import demPort
from agents.basic_Agent import agent as basicAgent
import logging
import argparse
import pandas as pd
import numpy as np


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--plz', type=int, required=False, default=6, help='PLZ-Agent')
    return parser.parse_args()


class demAgent(basicAgent):

    def __init__(self, date, plz):
        super().__init__(date=date, plz=plz, exchange='Market', typ='DEM')

        self.logger.info('Start des Agenten')
        # Aufbau des Portfolios mit den enstprechenden Haushalten, Gewerbe und Industrie
        self.portfolio = demPort(typ="DEM")                             # Keine Verwendung eines Solvers

        powerH0 = 0                                                     # Summe der bereits verwendeten Leistung [MW]

        # Einbindung der Prosumer PV mit Batterie
        pvBatteries = self.ConnectionMongo.getPVBatteries(plz)
        for key, value in pvBatteries.items():
            self.portfolio.addToPortfolio(key, {key: value})
            powerH0 += value['demandP']

        self.logger.info('Prosumer PV-Bat hinzugefügt')

        # Einbindung der Prosumer PV mit Wärmepumpe
        # pvHeatpumps = self.ConnectionMongo.getHeatPumps(plz)
        # for key, value in pvHeatpumps.items():
        #     self.portfolio.addToPortfolio(key, {key: value})
        #     powerH0 += value['demandP']
        #
        # self.logger.info('Prosumer PV-WP hinzugefügt')

        # Einbindung Consumer mit PV
        pv = self.ConnectionMongo.getPVs(plz)
        for key, value in pv.items():
            self.portfolio.addToPortfolio(key, {key: value})
            # powerH0 += value['demandP']

        self.logger.info('Consumer PV hinzugefügt')

        demand = self.ConnectionMongo.getDemand(plz)

        # Aufbau Standard Consumer H0
        name = 'plz_' + str(plz) + '_h0'
        self.portfolio.addToPortfolio(name, {name: {'demandP': np.round(demand['h0']*10**6 - powerH0, 2), 'typ': 'H0'}})
        self.logger.info('Consumer hinzugefügt')

        # Aufbau Standard Consumer G0
        name = 'plz_' + str(plz) + '_g0'
        self.portfolio.addToPortfolio(name, {name: {'demandP': np.round(demand['g0']*10**6, 2), 'typ': 'G0'}})
        self.logger.info('Gewerbe  hinzugefügt')

        # Aufbau Standard Consumer RLM
        name = 'plz_' + str(plz) + '_rlm'
        self.portfolio.addToPortfolio(name, {name: {'demandP': np.round(demand['rlm']*10**6, 2), 'typ': 'RLM'}})
        self.logger.info('Industrie hinzugefügt')

        if len(self.portfolio.energySystems) == 0:
            self.logger.info('Keine Kraftwerke im PLZ-Gebiet vorhanden')
            exit()

        self.logger.info('Aufbau des Agenten abgeschlossen')

    def optimize_balancing(self):
        """Einsatzplanung für den Regelleistungsmarkt"""
        self.logger.info('Planung Regelleistungsmarkt abgeschlossen')

    def optimize_dayAhead(self):
        """Einsatzplanung für den DayAhead-Markt"""
        orderbook = dict()                                                  # Oderbook für alle Gebote (Stunde 1-24)
        json_body = []                                                      # Liste zur Speicherung der Ergebnisse in der InfluxDB

        # Prognosen für den kommenden Tag
        weather = self.weatherForecast(self.date)                           # Wetterdaten (dir,dif,temp,wind)
        price = self.priceForecast(self.date)                               # Preisdaten (power,gas,nuc,coal,lignite)


        # Standardoptimierung
        self.portfolio.setPara(self.date, weather, price)
        self.portfolio.buildModel()
        power_dayAhead = np.asarray(self.portfolio.optimize(), np.float)  # Berechnung der Einspeiseleitung [kW]

        demand = dict(PvBat=(np.zeros_like(self.portfolio.t),np.zeros_like(self.portfolio.t)),      # Wärme- und Strombedarf Prosumer PV Bat
                      PvWp=(np.zeros_like(self.portfolio.t), np.zeros_like(self.portfolio.t)),      # Wärme- und Strombedarf Prosumer PV Wp
                      Pv=(np.zeros_like(self.portfolio.t), np.zeros_like(self.portfolio.t)),        # Wärme- und Strombedarf Consumer PV
                      H0=(np.zeros_like(self.portfolio.t), np.zeros_like(self.portfolio.t)),        # Wärme- und Strombedarf Consumer H0
                      G0=(np.zeros_like(self.portfolio.t), np.zeros_like(self.portfolio.t)),        # Wärme- und Strombedarf Consumer G0
                      RLM=(np.zeros_like(self.portfolio.t), np.zeros_like(self.portfolio.t)))       # Wärme- und Strombedarf Consumer RLM


        heatTotal = np.zeros_like(power_dayAhead)

        # aggregiere Energiesysteminformation
        for key, value in self.portfolio.energySystems.items():
            power = value['model'].powerDemand/10**3
            heat = value['model'].heatDemand/10**3
            for i in self.portfolio.t:
                heatTotal[i] += heat[i]
                demand[value['typ']][0][i] += heat[i]
                demand[value['typ']][1][i] += power[i]

        # Portfolioinformation
        time = self.date
        for i in self.portfolio.t:
            json_body.append(
                {
                    "measurement": 'Areas',
                    "tags": dict(typ='DEM',                                      # Typ Erneuerbare Energien
                                 agent=self.name,                                # Name des Agenten
                                 area=self.plz,                                  # Plz Gebiet
                                 timestamp='optimize_dayAhead'),                 # Zeitstempel der Tagesplanung
                    "time": time.isoformat() + 'Z',
                    "fields": dict(powerTotal=power_dayAhead[i]/10**3,           # Gesamte Nachfrage Strom  [MW]
                                   heatTotal=heatTotal[i]/10**3,                 # Gesamte Nachfrage Wärme  [MW]
                                   powerPvBat=demand['PvBat'][1][i],
                                   heatPvBat=demand['PvBat'][0][i],
                                   powerPvHp=demand['PvWp'][1][i],
                                   heatPvHp=demand['PvWp'][0][i],
                                   powerPvSolo=demand['Pv'][1][i],
                                   heatPvSolo=demand['Pv'][0][i],
                                   powerH0=demand['H0'][1][i],
                                   powerG0=demand['G0'][1][i],
                                   powerRLM=demand['RLM'][1][i])
                }
            )
            time = time + pd.DateOffset(hours=self.portfolio.dt)

        self.ConnectionInflux.saveData(json_body)

        # Aufbau der Gebotskurven
        for i in range(self.portfolio.T):
            orderbook.update({'h_%s' % i: {'quantity': [power_dayAhead[i]/10**3, 0], 'price': [3000, -3000], 'hour': i, 'name': self.name}})
        self.ConnectionMongo.setDayAhead(name=self.name, date=self.date, orders=orderbook)

        self.logger.info('Planung DayAhead-Markt abgeschlossen')

    def post_dayAhead(self):
        """Reaktion auf  die DayAhead-Ergebnisse"""
        json_body = []                                                          # Liste zur Speicherung der Ergebnisse in der InfluxDB

        # Abfrage der DayAhead Ergebnisse
        ask = self.ConnectionInflux.getDayAheadAsk(self.date, self.name)            # Angebotene Menge [MWh]
        bid = self.ConnectionInflux.getDayAheadBid(self.date, self.name)            # Nachgefragte Menge [MWh]
        price = self.ConnectionInflux.getDayAheadPrice(self.date)                   # MCP [€/MWh]
        profit = [float((ask[i] - bid[i]) * price[i]) for i in range(24)]           # erzielte Erlöse

        power_dayAhead = np.asarray(self.portfolio.optimize(), np.float)            # Berechnung der Einspeiseleitung [kW]

        demand = dict(PvBat=(np.zeros_like(self.portfolio.t),np.zeros_like(self.portfolio.t)),      # Wärme- und Strombedarf Prosumer PV Bat
                      PvWp=(np.zeros_like(self.portfolio.t), np.zeros_like(self.portfolio.t)),      # Wärme- und Strombedarf Prosumer PV Wp
                      Pv=(np.zeros_like(self.portfolio.t), np.zeros_like(self.portfolio.t)),        # Wärme- und Strombedarf Consumer PV
                      H0=(np.zeros_like(self.portfolio.t), np.zeros_like(self.portfolio.t)),        # Wärme- und Strombedarf Consumer H0
                      G0=(np.zeros_like(self.portfolio.t), np.zeros_like(self.portfolio.t)),        # Wärme- und Strombedarf Consumer G0
                      RLM=(np.zeros_like(self.portfolio.t), np.zeros_like(self.portfolio.t)))       # Wärme- und Strombedarf Consumer RLM

        heatTotal = np.zeros_like(power_dayAhead)

        # aggregiere Energiesysteminformation
        for key, value in self.portfolio.energySystems.items():
            power = value['model'].powerDemand/10**3
            heat = value['model'].heatDemand/10**3
            for i in self.portfolio.t:
                heatTotal[i] += heat[i]
                demand[value['typ']][0][i] += heat[i]
                demand[value['typ']][1][i] += power[i]

        # Portfolioinformation
        time = self.date
        for i in self.portfolio.t:
            json_body.append(
                {
                    "measurement": 'Areas',
                    "tags": dict(typ='DEM',                                      # Typ Erneuerbare Energien
                                 agent=self.name,                                # Name des Agenten
                                 area=self.plz,                                  # Plz Gebiet
                                 timestamp='post_dayAhead'),                     # Zeitstempel der Tagesplanung
                    "time": time.isoformat() + 'Z',
                    "fields": dict(powerTotal=power_dayAhead[i]/10**3,           # Gesamte Nachfrage Strom  [MW]
                                   heatTotal=heatTotal[i]/10**3,                 # Gesamte Nachfrage Wärme  [MW]
                                   powerPvBat=demand['PvBat'][1][i],
                                   heatPvBat=demand['PvBat'][0][i],
                                   powerPvHp=demand['PvWp'][1][i],
                                   heatPvHp=demand['PvWp'][0][i],
                                   powerPvSolo=demand['Pv'][1][i],
                                   heatPvSolo=demand['Pv'][0][i],
                                   powerH0=demand['H0'][1][i],
                                   powerG0=demand['G0'][1][i],
                                   powerRLM=demand['RLM'][1][i],
                                   profit=profit[i])
                }
            )
            time = time + pd.DateOffset(hours=self.portfolio.dt)

        self.ConnectionInflux.saveData(json_body)

        self.logger.info('DayAhead Ergebnisse erhalten')

    def optimize_actual(self):
        """Abruf Prognoseabweichung und Übermittlung der Fahrplanabweichung"""
        # TODO: Überarbeitung, wenn Regelleistung
        schedule = self.ConnectionInflux.getPowerScheduling(self.date, self.name, 'post_dayAhead')
        # Berechnung der Prognoseabweichung
        actual = np.asarray(self.portfolio.fixPlaning()/10**3, np.float).reshape((-1,))
        # Wenn kein Zuschlag am DayAhead Markt vorliegt, passe Nachfrage an
        difference = np.asarray([(schedule[i] - actual[i] if schedule[i] > 0 else 0.00) for i in self.portfolio.t])
        power = np.asarray([(actual[i] if schedule[i] > 0 else 0) for i in self.portfolio.t])
        # Aufbau der "Gebote" (Abweichungen zum gemeldeten Fahrplan)
        orderbook = dict()
        for i in range(self.portfolio.T):
            orderbook.update({'h_%s' % i: {'quantity': difference[i], 'hour': i, 'name': self.name}})
        self.ConnectionMongo.setActuals(name=self.name, date=self.date, orders=orderbook)
        # Abspeichern der Ergebnisse
        json_body = []
        time = self.date
        for i in self.portfolio.t:
            json_body.append(
                {
                    "measurement": 'Areas',
                    "tags": dict(agent=self.name, area=self.plz, timestamp='optimize_actual', typ='DEM'),
                    "time": time.isoformat() + 'Z',
                    "fields": dict(Difference=difference[i], power=power[i])
                }
            )
            time = time + pd.DateOffset(hours=self.portfolio.dt)
        self.ConnectionInflux.saveData(json_body)

        self.logger.info('Aktuellen Fahrplan erstellt')

    def post_actual(self):
        """ Abschlussplanung des Tages """
        # TODO: Überarbeitung, wenn Regelleistung
        # power = self.ConnectionInflux.getPowerScheduling(self.date, self.name, 'optimize_actual')  # Letzter bekannter  Fahrplan
        #
        # # Abspeichern der Ergebnisse
        # time = self.date
        # json_body = []
        # for i in self.portfolio.t:
        #     json_body.append(
        #         {
        #             "measurement": 'Areas',
        #             "tags": dict(agent=self.name, area=self.plz, timestamp='post_actual', typ='DEM'),
        #             "time": time.isoformat() + 'Z',
        #             "fields": dict(power=power[i])
        #         }
        #     )
        #     time = time + pd.DateOffset(hours=self.portfolio.dt)
        # self.ConnectionInflux.saveData(json_body)

        # Planung für den nächsten Tag
        # Anpassung der Prognosemethoden für den Verbrauch und die Preise
        for key, method in self.forecasts.items():
            if key != 'weather':
                method.collectData(self.date)
                method.counter += 1
                if method.counter >= method.collect:
                    method.fitFunction()
                    method.counter = 0

        self.logger.info('Tag %s abgeschlossen' % self.date)

        print('Agent %s %s done' % (self.name, self.date.date()))

if __name__ == "__main__":

    args = parse_args()
    agent = demAgent(date='2019-01-01', plz=args.plz)
    agent.ConnectionMongo.login(agent.name, False)
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
