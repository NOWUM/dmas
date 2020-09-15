import numpy as np
import pandas as pd
from gurobipy import *

class port_model:

    def __init__(self, T=24, dt=1, gurobi=False, date='2020-01-01', typ='RES'):

        self.date = pd.to_datetime(date)                                    # Aktueller Tag der der Optimierung

        # Einstellung der Standardlastprofile
        # -- > Wärmebedarf
        self.Ref_Temperature = np.asarray(np.load(open(r'./data/Ref_Temp.array','rb')), np.float32)
        self.Ref_factors = np.asarray(np.load(open(r'./data/Ref_Factors.array','rb')), np.float32)
        # -- > Strombedarf
        self.Ref_H0 = np.asarray(np.load(open(r'./data/Ref_H0.array','rb')), np.float32)
        self.Ref_G0 = np.asarray(np.load(open(r'./data/Ref_G0.array','rb')), np.float32)
        self.Ref_Rlm = np.asarray(np.load(open(r'./data/Ref_RLM.array','rb')), np.float32)

        self.energySystems = {}                                             # Verwaltung der Energiesysteme
        self.typ = typ                                                      # Portfoliotyp (DEM,RES,PWP,...)
        self.capacities = dict(wind=0, solar=0, fossil=0, water=0, bio=0,
                               lignite=0, gas=0, coal=0, nuc=0)             # installierte Erzeugungskapazitäten

        # Meta Daten Zeitintervalle
        self.T = T                                                          # Anzahl an Zeitschritten
        self.t = np.arange(T)                                               # Array mit Zeitschritten
        self.dt = dt                                                        # Zeitschrittlänge

        self.power = np.zeros(T)                                            # Leistung am Netzbezugspunkt

        self.emission = np.zeros(T)                                         # Kosten aus CO2 Emissionen
        self.fuel = np.zeros(T)                                             # Brennstoffkosten

        self.generation = dict(powerTotal=np.zeros_like(self.t, dtype=float),                 # Erzeugung Gesamt
                               powerSolar=np.zeros_like(self.t, dtype=float),                 # Erzeugung aus Solar
                               powerWind=np.zeros_like(self.t, dtype=float),                  # Erzeugung aus Wind
                               powerWater=np.zeros_like(self.t, dtype=float),                 # Erzeugung aus Wasserkraft
                               powerBio=np.zeros_like(self.t, dtype=float),                   # Erzeugung aus Biomasse
                               powerLignite=np.zeros_like(self.t, dtype=float),               # Erzeugung aus Braunkohle
                               powerCoal=np.zeros_like(self.t, dtype=float),                  # Erzeugung aus Steinkohle
                               powerGas=np.zeros_like(self.t, dtype=float),                   # Erzeugung aus Erdgas
                               powerNuc=np.zeros_like(self.t, dtype=float))                   # Erzeugung aus Kernkraft

        self.demand = dict(power=np.zeros_like(self.t, dtype=float),                     # Strombedarf
                                heat=np.zeros_like(self.t, dtype=float))                 # Wärmebedarf

        # Optimierungsrelevante Parameter
        self.weather = {}                                                   # Wetterdaten (wind,dir,dif,temp)
        self.prices = {}                                                    # Preiserwartung
        self.frcstDemand = []                                               # Lasterwartung

        # GGLP-Model für den Dispatch der Kraftwerke
        if gurobi:
            self.m = Model('aggregation')
            self.m.Params.OutputFlag = 0
            self.m.Params.TimeLimit = 30
            self.m.Params.MIPGap = 0.05
            self.m.__len__ = 1

    # ----- Set Parameter for optimization -----
    def setPara(self, date, weather, prices, demand=np.zeros(24)):
        self.date = pd.to_datetime(date)
        self.weather = weather
        self.prices = prices
        self.frcstDemand = demand


    # ----- Add Energysystem to Portfolio -----
    def addToPortfolio(self, name, energysystem):
        pass

    # ----- build model with constrains for optimization -----
    def buildModel(self, response=[]):
        pass

    def optimize(self):
        power = np.zeros_like(self.t)
        return power

    def getActual(self):
        power = np.zeros_like(self.t)
        return power

    def fixPlaning(self):
        power = np.zeros_like(self.t)
        return power

if __name__ == "__main__":
    test = port_model()
    pass


