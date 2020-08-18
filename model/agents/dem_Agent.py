import os
os.chdir(os.path.dirname(os.path.dirname(__file__)))
from aggregation.dem_Port import demPort
from agents.basic_Agent import agent as basicAgent
import argparse
import pandas as pd
import numpy as np


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--plz', type=int, required=False, default=15, help='PLZ-Agent')
    return parser.parse_args()


class demAgent(basicAgent):

    def __init__(self, date, plz):
        super().__init__(date=date, plz=plz, exchange='Market', typ='DEM')
        # Aufbau des Portfolios mit den enstprechenden Haushalten, Gewerbe und Industrie
        self.logger.info('Start des Agenten')
        self.portfolio = demPort(typ="DEM")

        # Aufbau der Prosumer mit PV und Batterie
        for key, value in self.ConnectionMongo.getPVBatteries().items():
            self.portfolio.addToPortfolio('PvBat' + str(key), {'PvBat' + str(key): value})
        self.logger.info('Prosumer PV-Bat hinzugefügt')

        # Aufbau Consumer mit PV
        for key, value in self.ConnectionMongo.getPVs().items():
            self.portfolio.addToPortfolio('Pv' + str(key), {'Pv' + str(key): value})
        self.logger.info('Consumer PV hinzugefügt')

        demand = self.ConnectionMongo.getDemand()

        # Aufbau Standard Consumer H0
        name = 'plz_' + str(plz) + '_h0'
        self.portfolio.addToPortfolio(name, {name: {'demandP': np.round(demand['h0']*10**6, 2), 'typ': 'H0'}})
        self.logger.info('Consumer hinzugefügt')

        # Aufbau Standard Consumer G0
        name = 'plz_' + str(plz) + '_g0'
        self.portfolio.addToPortfolio(name, {name: {'demandP': np.round(demand['g0']*10**6, 2), 'typ': 'G0'}})
        self.logger.info('Gewerbe  hinzugefügt')

        # Aufbau Standard Consumer RLM
        name = 'plz_' + str(plz) + '_rlm'
        self.portfolio.addToPortfolio(name, {name: {'demandP': np.round(demand['rlm']*10**6, 2), 'typ': 'RLM'}})
        self.logger.info('Industrie hinzugefügt')

        # Wenn keine Energiesysteme vorhanden sind, beende den Agenten
        if len(self.portfolio.energySystems) == 0:
            print('Nummer: %s Keine Energiesysteme im Energiesystem' % plz)
            print(' --> Aufbau des Agenten %s_%s beendet' % (self.typ, plz))
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

        # Portfolioinformation
        time = self.date
        for i in self.portfolio.t:
            json_body.append(
                {
                    "measurement": 'Areas',
                    "tags": dict(typ='DEM',                                                         # Typ Nachfrage
                                 agent=self.name,                                                   # Name des Agenten
                                 area=self.plz,                                                     # Plz Gebiet
                                 timestamp='optimize_dayAhead'),                                    # Zeitstempel
                    "time": time.isoformat() + 'Z',
                    "fields": dict(powerTotal=power_dayAhead[i]/10**3,                              # Gesamte Nachfrage Strom  [MW]
                                   heatTotal=self.portfolio.demand['heat'][i]/10**3,                # Gesamte Nachfrage Wärme  [MW]
                                   powerSolar=self.portfolio.generation['solar'][i]/10**3)          # Gesamte Erzeugung aus Solar [MW]
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
        json_body = []                                                              # Liste zur Speicherung der Ergebnisse in der InfluxDB

        # Abfrage der DayAhead Ergebnisse
        ask = self.ConnectionInflux.getDayAheadAsk(self.date, self.name)            # Angebotene Menge [MWh]
        bid = self.ConnectionInflux.getDayAheadBid(self.date, self.name)            # Nachgefragte Menge [MWh]
        price = self.ConnectionInflux.getDayAheadPrice(self.date)                   # MCP [€/MWh]
        profit = [float((ask[i] - bid[i]) * price[i]) for i in range(24)]           # erzielte Erlöse

        power_dayAhead = np.asarray(self.portfolio.optimize(), np.float)            # Berechnung der Einspeiseleitung [kW]

        # Portfolioinformation
        time = self.date
        for i in self.portfolio.t:
            json_body.append(
                {
                    "measurement": 'Areas',
                    "tags": dict(typ='DEM',                                         # Typ Erneuerbare Energien
                                 agent=self.name,                                   # Name des Agenten
                                 area=self.plz,                                     # Plz Gebiet
                                 timestamp='post_dayAhead'),                        # Zeitstempel der Tagesplanung
                    "time": time.isoformat() + 'Z',

                    "fields": dict(powerTotal=power_dayAhead[i] / 10 ** 3,                      # Gesamte Nachfrage Strom  [MW]
                                   heatTotal=self.portfolio.demand['heat'][i] / 10 ** 3,        # Gesamte Nachfrage Wärme  [MW]
                                   powerSolar=self.portfolio.generation['solar'][i] / 10 ** 3,  # Gesamte Erzeugung aus Solar [MW]
                                   profit=profit[i])
                })
            time = time + pd.DateOffset(hours=self.portfolio.dt)

        self.ConnectionInflux.saveData(json_body)
        self.logger.info('DayAhead Ergebnisse erhalten')

    def optimize_actual(self):
        """Abruf Prognoseabweichung und Übermittlung der Fahrplanabweichung"""
        # TODO: Überarbeitung, wenn Regelleistung
        self.logger.info('Aktuellen Fahrplan erstellt')

    def post_actual(self):
        """ Abschlussplanung des Tages """
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
        else:
            self.delay -= 1

        self.logger.info('Tag %s abgeschlossen' % self.date)
        print('Agent %s %s done' % (self.name, self.date.date()))

if __name__ == "__main__":

    args = parse_args()
    agent = demAgent(date='2019-01-01', plz=args.plz)
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
