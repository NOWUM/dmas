import numpy as np
from components.basic_EnergySystem import energySystem as es

class h0_model(es):
    """ Haushalte (H0) in einem Versorgungsgebiet """
    def __init__(self,
                 t=np.arange(24), T=24, dt=1,                                                       # Metainfo Zeit
                 demQ=1000, demP=3000,                                                              # Jahresverbräuche
                 refSLP=np.asarray(np.load(open(r'./data/Ref_H0.array','rb')), np.float32),         # SLP Strom
                 refTemp=np.asarray(np.load(open(r'./data/Ref_Temp.array', 'rb')), np.float32),     # Referenztemperatur
                 factors=np.asarray(np.load(open(r'./data/Ref_Factors.array', 'rb')), np.float32),  # Stundenfaktoren
                 parameters=np.asarray([2.8, -37, 5.4, 0.17], np.float32)):                         # Gebäudeparameter
        super().__init__(t, T, dt, demQ, demP, refSLP, refTemp, factors, parameters)

    def getPowerDemand(self, data, date):
        power = data['demandP'] * 0.1           # Variabler Anteil
        base = data['demandP'] * 0.9            # Konstanter Anteil
        # --> Anpassung Aufgrund der überschätzten Gleichzeitigkeit in den SLP
        # --> die Faktoren wurden empirisch ermittelt und sind eine grobe Schätzung

        demand = self.slpP.get_profile(date.dayofyear, date.dayofweek, power).reshape((96, 1))
        demand = np.asarray([np.mean(demand[i:i+3]) for i in range(0,96,4)], np.float).reshape((-1,)) + base / 8760

        return demand

    def build(self, data, ts, date):
        self.powerDemand = self.getPowerDemand(data, date)

class g0_model(es):
    """ Gewerbe (G0) in einem Versorgungsgebiet """
    def __init__(self,
                 t=np.arange(24), T=24, dt=1,                                                       # Metainfo Zeit
                 demQ=1000, demP=3000,                                                              # Jahresverbräuche
                 refSLP=np.asarray(np.load(open(r'./data/Ref_G0.array','rb')), np.float32),         # SLP Strom
                 refTemp=np.asarray(np.load(open(r'./data/Ref_Temp.array', 'rb')), np.float32),     # Referenztemperatur
                 factors=np.asarray(np.load(open(r'./data/Ref_Factors.array', 'rb')), np.float32),  # Stundenfaktoren
                 parameters=np.asarray([2.8, -37, 5.4, 0.17], np.float32)):                         # Gebäudeparameter
        super().__init__(t, T, dt, demQ, demP, refSLP, refTemp, factors, parameters)

    def getPowerDemand(self, data, date):
        power = data['demandP'] * 0.1           # Variabler Anteil
        base = data['demandP'] * 0.9            # Konstanter Anteil
        # --> Anpassung Aufgrund der überschätzten Gleichzeitigkeit in den SLP
        # --> die Faktoren wurden empirisch ermittelt und sind eine grobe Schätzung

        demand = self.slpP.get_profile(date.dayofyear, date.dayofweek, power).reshape((96, 1))
        demand = np.asarray([np.mean(demand[i:i + 3]) for i in range(0, 96, 4)], np.float).reshape((-1,)) + base / 8760

        return demand

    def build(self, data, ts, date):
        self.powerDemand = self.getPowerDemand(data, date)

class rlm_model(es):
    """ Industrie (RLM) in einem Versorgungsgebiet """
    def __init__(self,
                 t=np.arange(24), T=24, dt=1,                                                       # Metainfo Zeit
                 demQ=1000, demP=3000,                                                              # Jahresverbräuche
                 refSLP=np.asarray(np.load(open(r'./data/Ref_RLM.array','rb')), np.float32),        # SLP Strom
                 refTemp=np.asarray(np.load(open(r'./data/Ref_Temp.array', 'rb')), np.float32),     # Referenztemperatur
                 factors=np.asarray(np.load(open(r'./data/Ref_Factors.array', 'rb')), np.float32),  # Stundenfaktoren
                 parameters=np.asarray([2.8, -37, 5.4, 0.17], np.float32)):                         # Gebäudeparameter
        super().__init__(t, T, dt, demQ, demP, refSLP, refTemp, factors, parameters)

    def getPowerDemand(self, data, date):
        power = data['demandP'] * 0.1           # Variabler Anteil
        base = data['demandP'] * 0.9            # Konstanter Anteil
        # --> Anpassung Aufgrund der überschätzten Gleichzeitigkeit in den SLP
        # --> die Faktoren wurden empirisch ermittelt und sind eine grobe Schätzung

        demand = self.slpP.get_profile(date.dayofyear, date.dayofweek, power).reshape((96, 1))
        demand = np.asarray([np.mean(demand[i:i + 3]) for i in range(0, 96, 4)], np.float).reshape((-1,)) + base / 8760

        return demand

    def build(self, data, ts, date):
        self.powerDemand = self.getPowerDemand(data, date)

if __name__ == "__main__":
    testh0 = h0_model()
    testg0 = g0_model()
    testRlm = rlm_model()
