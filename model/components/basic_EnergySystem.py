import numpy as np
from apps.slpP import slpGen as slpP
from apps.slpQ import slpGen as slpQ


class energySystem:
    """
    Basis Energiesystem \n
    Es beinhaltet die westentlichen Bestandteile eines Energiesystems: \n
    - Standardlastprofil Strom (default H0)
    - Standardlastprofil Wärme (Algorithmus TU München)
    - Umrechnungsfaktoren für den stündlichen Wärmebedarf (Algorithmus TU München)
    - Referenztemperatur (Wetterjahr 2017 veröffentlicht vom DWD)
    - Paramtersatz des Gebäudes (default [2.8, -37, 5.4, 0.17] --> Gebäude um 1990)
    - Wärmebedarf pro Jahr in kWh/a (default 1000 kWh/a)
    - Strombedarf pro Jahr in kWh/a (default 3000 kWh/a)

    Ebenfalls werden meta Daten der zeitlichen Auflösung festgehalten:
    - Zeitschritte pro Tag (default 24)
    - Array mit Zeitschritten (default [0...23])
    - Zeitschrittlänge (default 1)
    """
    def __init__(self,
                 t=np.arange(24), T=24, dt=1,                                                       # Metainfo Zeit
                 demQ=1000, demP=3000,                                                              # Jahresverbräuche
                 refSLP=np.asarray(np.load(open(r'./data/Ref_H0.array','rb')), np.float32),         # SLP Strom
                 refTemp=np.asarray(np.load(open(r'./data/Ref_Temp.array', 'rb')), np.float32),     # Referenztemperatur
                 factors=np.asarray(np.load(open(r'./data/Ref_Factors.array', 'rb')), np.float32),  # Stundenfaktoren
                 parameters=np.asarray([2.8, -37, 5.4, 0.17], np.float32), typ=0):                  # Gebäudeparameter

        # Meta Daten Zeitintervalle
        self.t = t                  # Array mit Zeitschritten
        self.T = T                  # Anzahl an Zeitschritten
        self.dt = dt                # Zeitschrittlänge

        # Verwendetes Standardlastprofil Strom
        self.slpP = slpP(typ=typ, refSLP=refSLP)

        # Wärmelastprofil mit entsprechenden Parametern
        self.slpQ = slpQ(demandQ=demQ, parameters=np.asarray(parameters, np.float32).reshape((-1,)),
                         refTemp=np.asarray(refTemp,np.float32).reshape((-1,)),
                         factors=np.asarray(factors,np.float32).reshape((24, -1)))

        # Zeitreiheninformationen
        self.powerYear = demP                                   # Strombedarf pro Jahr in kWh/a
        self.heatYear = demQ                                    # Wärmebedarf pro Jahr in kWh/a

        self.demand = dict(power=np.zeros_like(self.t),         # Strombedarf der Komponente in kW
                           heat=np.zeros_like(self.t))          # Wärmebedarf der Komponente in kW

        self.generation = dict(wind=np.zeros_like(self.t),      # Winderzeugung der Komponente
                               solar=np.zeros_like(self.t),     # PV-Erzeugung der Komponente
                               fossil=np.zeros_like(self.t),    # Erzeugung aus konv. Kraftwerken der Komponente
                               bio=np.zeros_like(self.t),       # Erzeugung aus Biomasse
                               water=np.zeros_like(self.t),     # Erzeugung aus Laufwasserkraftwerken
                               storage=np.zeros_like(self.t))   # Speicherleistung der Komponente

        self.power = np.zeros_like(self.t)                      # Leistung am Netzbezugspunkt
        self.volume = np.zeros_like(self.t)
        self.emission = np.zeros_like(self.t)
        self.fuel = np.zeros_like(self.t)


    def getPowerDemand(self, data, date):
        """ Stromverbrauch des aktuellen Tages in stündlicher Auflösung und [kW] """
        demand = self.slpP.get_profile(date.dayofyear, date.dayofweek, data['demandP']).reshape((96, 1))
        demand = np.asarray([np.mean(demand[i:i+3]) for i in range(0, 96, 4)], np.float).reshape((-1,))
        return demand

    def getHeatDemand(self, data, ts):
        """ Wärmebedarf des aktuellen Tages in Abhängigkeit des aktuellen Wetters in stündlicher Auflösung und [kW] """
        return self.slpQ.get_profile(np.asarray(ts['temp'], np.float32))

    def build(self, name, data, ts):
        """ Berechung der Zeitreiheninformationen (Strombedarf, Wärmebedarf, Erzeugung, ...) der Komponente """
        pass

