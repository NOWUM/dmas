# third party modules
import os
import gurobipy as gby
import numpy as np


# model modules
from components.energy_system import EnergySystem
os.chdir(os.path.dirname(os.path.dirname(__file__)))


class Storage(EnergySystem):

    def __init__(self, t=np.arange(24), T=24, dt=1, name='default', storage=None):
        super().__init__(t, T, dt)

        # initialize default power plant
        if storage is None:
            storage = {"eta+": 0.85, "eta-": 0.8, "fuel": "water", "V0": 4230, "VMin": 0, "VMax": 8460,
                       "P+_Max": 1060, "P-_Max": 1060,"P+_Min": 0, "P-_Min": 0}
        self.storage = storage
        self.name = name  # storage name

        # initialize gurobi model for optimization
        self.m = gby.Model('storage')
        self.m.Params.OutputFlag = 0
        self.m.Params.TimeLimit = 30
        self.m.Params.MIPGap = 0.05
        self.m.__len__ = 1

        self.volume = np.zeros_like(self.t, np.float)

    def initialize_model(self, model, name):

        # power and volume
        power = model.addVars(self.t, vtype=gby.GRB.CONTINUOUS, name='P_' + name, lb=-gby.GRB.INFINITY, ub=gby.GRB.INFINITY)
        volume = model.addVars(self.t, vtype=gby.GRB.CONTINUOUS, name='V_' + name, lb=self.storage['VMin'],
                               ub=self.storage['VMax'])

        pP = model.addVars(self.t, vtype=gby.GRB.CONTINUOUS, name='P+_' + name, lb=0, ub=self.storage['P+_Max'])
        pM = model.addVars(self.t, vtype=gby.GRB.CONTINUOUS, name='P-_' + name, lb=0, ub=self.storage['P-_Max'])
        on = model.addVars(self.t, vtype=gby.GRB.BINARY, name='On_' + name)

        # profit
        profit = model.addVars(self.t, vtype=gby.GRB.CONTINUOUS, name='Profit_' + name, lb=-gby.GRB.INFINITY, ub=gby.GRB.INFINITY)
        model.addConstrs(profit[i] == power[i] * self.prices['power'][i] for i in self.t)

        # power = charge + discharge
        model.addConstrs(power[i] == -pP[i] + pM[i] for i in self.t)
        # power limits
        model.addConstrs(pP[i] <= on[i] * self.storage['P+_Max'] for i in self.t)
        model.addConstrs(pP[i] >= on[i] * self.storage['P+_Min'] for i in self.t)
        model.addConstrs(pM[i] <= (1 - on[i]) * self.storage['P-_Max'] for i in self.t)
        model.addConstrs(pM[i] >= (1 - on[i]) * self.storage['P-_Min'] for i in self.t)

        # volume restriction
        model.addConstr(volume[0] == self.storage['V0'] + self.dt *
                        (self.storage['eta+'] * pP[0] - pM[0] / self.storage['eta-']))
        model.addConstrs(volume[i] == volume[i-1] + self.dt *
                         (self.storage['eta+'] * pP[i] - pM[i] / self.storage['eta-']) for i in self.t[1:])

        model.setObjective(gby.quicksum(profit[i] for i in self.t), gby.GRB.MAXIMIZE)

        model.update()

    def optimize(self):

        self.initialize_model(self.m, self.name)

        self.m.optimize()

        self.power = np.asarray([p.x for p in [x for x in self.m.getVars() if 'P_' in x.VarName]]).reshape((-1,))
        self.volume = np.asarray([p.x for p in [x for x in self.m.getVars() if 'V_' in x.VarName]]).reshape((-1,))
        self.generation['power' + self.storage['fuel'].capitalize()] = self.power

        return self.power


if __name__ == "__main__":
    st = Storage()

    power_price = 300 * np.ones(24)
    power_price[:12] = power_price[12:] - 150
    co = np.ones(24) * 23.8 * np.random.uniform(0.95, 1.05, 24)  # -- Emission Price     [€/t]
    gas = np.ones(24) * 24.8 * np.random.uniform(0.95, 1.05, 24)  # -- Gas Price          [€/MWh]
    lignite = 1.5 * np.random.uniform(0.95, 1.05)  # -- Lignite Price      [€/MWh]
    coal = 9.9 * np.random.uniform(0.95, 1.05)  # -- Hard Coal Price    [€/MWh]
    nuc = 1.0 * np.random.uniform(0.95, 1.05)  # -- nuclear Price      [€/MWh]

    prices = dict(power=power_price, gas=gas, co=co, lignite=lignite, coal=coal, nuc=nuc)

    st.set_parameter(date='2018-01-01', weather=None,
                     prices=prices)

    x = st.optimize()