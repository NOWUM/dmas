import numpy as np
import pandas as pd
from gurobipy import *

class port_model:

    def __init__(self, T=24, dt=1, gurobi=False, date='2020-01-01', typ='RES'):

        # -- meta data
        self.date = pd.to_datetime(date)            # -- Date-Time
        self.energySystems = {}                     # -- dict to save the meta data for the systems
        self.typ = typ                              # -- Portfoliotyp (DEM,RES,PWP,...)
        self.Cap_Wind = 0                           # -- Wind Capacity in Portfolio
        self.Cap_Solar = 0                          # -- Solar Capacity in Portfolio
        self.Cap_PWP = 0                            # -- Power Plant Capacity in Portfolio
        # time data
        self.T = 24                                 # -- steps per day
        self.t = np.arange(T)                       # -- array with single steps
        self.dt = dt                                # -- resolution

        self.power = np.zeros_like(self.t)
        self.pSolar = np.zeros_like(self.t)
        self.pWind = np.zeros_like(self.t)

        # -- optimization data
        self.weather = {}                           # -- weahter data dict(wind,dir,dif,temp)
        self.prices = {}                            # -- price data dict(power,co,gas,lignite,coal,nuc)
        self.posBalPower = []                       # -- postive Balancing Power
        self.negBalPower = []                       # -- negative Balancing Power
        self.demand = []

        # SLP Power & Heat Data
        self.Ref_Temperature = np.asarray(np.load(open(r'./data/Ref_Temp.array','rb')), np.float32)
        self.Ref_factors = np.asarray(np.load(open(r'./data/Ref_Factors.array','rb')), np.float32)

        self.Ref_H0 = np.asarray(np.load(open(r'./data/Ref_H0.array','rb')), np.float32)
        self.Ref_G0 = np.asarray(np.load(open(r'./data/Ref_G0.array','rb')), np.float32)
        self.Ref_Rlm = np.asarray(np.load(open(r'./data/Ref_RLM.array','rb')), np.float32)
        self.m = {}

        # optimization model
        if gurobi:
            self.m = Model('aggregation')             # -- gurobi model for milp model
            self.m.Params.OutputFlag = 0              # -- hide output

    # ----- Set Parameter for optimization -----
    def setPara(self, date, weather, prices, demand=np.zeros(24), posBalPower=np.zeros(24), negBalPower=np.zeros(24)):
        self.date = pd.to_datetime(date)
        self.weather = weather
        self.prices = prices
        self.demand = demand
        self.posBalPower = np.asarray(posBalPower)
        self.negBalPower = np.asarray(negBalPower)

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


