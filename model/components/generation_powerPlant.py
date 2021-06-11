# third party modules
import os
from gurobipy import *
import numpy as np


# model modules
from components.energy_system import EnergySystem
os.chdir(os.path.dirname(os.path.dirname(__file__)))


class PowerPlant(EnergySystem):

    def __init__(self, t=np.arange(24), T=24, dt=1, name='default', power_plant=None):
        super().__init__(t, T, dt)

        # initialize default power plant
        if power_plant is None:
            power_plant = dict(fuel='lignite', maxPower=10, minPower=5, eta=0.4, P0=5, chi=0.407,
                               stopTime=7, runTime=6, gradP=1, gradM=1, on=1, off=0)
        self.power_plant = power_plant
        self.name = name  # power plant block name

        # initialize gurobi model for optimization
        self.m = Model('power_plant')
        self.m.Params.OutputFlag = 0
        self.m.Params.TimeLimit = 30
        self.m.Params.MIPGap = 0.05
        self.m.__len__ = 1

        # determine start up cost
        # (Konventionelle Kraftwerke Technologiesteckbrief zur Analyse
        # „Flexibilitätskonzepte für die Stromversorgung 2050“)
        self.start_cost = 0
        if power_plant['fuel'] == 'gas':
            self.start_cost = 60 * power_plant['maxPower']
        if power_plant['fuel'] == 'lignite':
            if power_plant['maxPower'] > 500:
                self.start_cost = 60 * power_plant['maxPower']
            else:
                self.start_cost = 50 * power_plant['maxPower']
        if power_plant['fuel'] == 'coal':
            if power_plant['maxPower'] > 500:
                self.start_cost = 60 * power_plant['maxPower']
            else:
                self.start_cost = 50 * power_plant['maxPower']
        if power_plant['fuel'] == 'nuc':
            self.start_cost = 250 * power_plant['maxPower']

        self.power, self. emission, self.fuel, self.start = np.zeros_like(self.t, np.float), \
                                                            np.zeros_like(self.t, np.float), \
                                                            np.zeros_like(self.t, np.float), \
                                                            np.zeros_like(self.t, np.float)

    def initialize_model(self, model, name):

        delta = self.power_plant['maxPower'] - self.power_plant['minPower']
        su = self.power_plant['minPower']
        sd = self.power_plant['minPower']

        p_out = model.addVars(self.t, vtype=GRB.CONTINUOUS, name='P_' + name, lb=0, ub=self.power_plant['maxPower'])
        # power at each time t
        model.addConstr(p_out[0] <= self.power_plant['P0'] + self.power_plant['gradP'])
        model.addConstr(p_out[0] >= self.power_plant['P0'] - self.power_plant['gradM'])
        # power corresponding to the optimization
        p_opt = model.addVars(self.t, vtype=GRB.CONTINUOUS, name='opt_', lb=0, ub=delta)

        # states (on, ramp up, ramp down)
        z = model.addVars(self.t, vtype=GRB.BINARY, name='z_' + name)
        v = model.addVars(self.t, vtype=GRB.BINARY, name='v_' + name)
        w = model.addVars(self.t, vtype=GRB.BINARY, name='w_' + name)
        model.addConstrs(p_out[i] == p_opt[i] + z[i] * self.power_plant['minPower'] for i in self.t)

        # power limits
        model.addConstrs(0 <= p_opt[i] for i in self.t)
        model.addConstrs(p_opt[i] <= delta * z[i] for i in self.t)
        model.addConstrs(p_opt[i] <= delta * z[i] - (self.power_plant['maxPower'] - su) * v[i]
                         - (self.power_plant['maxPower'] - sd) * w[i + 1] for i in self.t[:-1])

        # power gradients
        model.addConstrs(p_opt[i] - p_opt[i-1] <= self.power_plant['gradP'] * z[i-1] for i in self.t[1:])
        model.addConstrs(p_opt[i-1] - p_opt[i] <= self.power_plant['gradM'] * z[i] for i in self.t[1:])

        # run- & stop-times
        model.addConstrs(1-z[i] >= quicksum(w[k] for k in range(max(0, i+1 - self.power_plant['stopTime']), i))
                         for i in self.t)
        model.addConstrs(z[i] >= quicksum(v[k] for k in range(max(0, i+1 - self.power_plant['runTime']), i))
                         for i in self.t)
        model.addConstrs(z[i - 1] - z[i] + v[i] - w[i] == 0 for i in self.t[1:])

        # initialize stat
        if self.power_plant['on'] > 0:
            model.addConstrs(z[i] == 1 for i in range(0, self.power_plant['runTime']
                                                      - self.power_plant['on']))
        else:
            model.addConstrs(z[i] == 0 for i in range(0, self.power_plant['stopTime']
                                                      - self.power_plant['off']))
        # fuel cost
        fuel = model.addVars(self.t, vtype=GRB.CONTINUOUS, name='F_' + name, lb=-GRB.INFINITY, ub=GRB.INFINITY)
        if self.power_plant['fuel'] == 'lignite':
            model.addConstrs(fuel[i] == p_out[i] / self.power_plant['eta'] * self.prices['lignite'] for i in self.t)
        if self.power_plant['fuel'] == 'coal':
            model.addConstrs(fuel[i] == p_out[i] / self.power_plant['eta'] * self.prices['coal'] for i in self.t)
        if self.power_plant['fuel'] == 'gas':
            model.addConstrs(fuel[i] == p_out[i] / self.power_plant['eta'] * self.prices['gas'][i] for i in self.t)
        if self.power_plant['fuel'] == 'nuc':
            model.addConstrs(fuel[i] == p_out[i] / self.power_plant['eta'] * self.prices['nuc'] for i in self.t)

        # emission cost
        emission = model.addVars(self.t, vtype=GRB.CONTINUOUS, name='E_' + name, lb=0, ub=GRB.INFINITY)
        model.addConstrs(emission[i] == p_out[i] * self.power_plant['chi'] / self.power_plant['eta']
                         * self.prices['co'][i] for i in self.t)

        # start cost
        start_up = model.addVars(self.t, vtype=GRB.CONTINUOUS, name='S_' + name, lb=0, ub=GRB.INFINITY)
        model.addConstrs(start_up[i] == v[i] * self.start_cost for i in self.t)

        profit = model.addVar(vtype=GRB.CONTINUOUS, name='Profit' + name, lb=-GRB.INFINITY, ub=GRB.INFINITY)
        model.addConstr(profit == quicksum(p_out[i] * self.prices['power'][i] for i in self.t))
        model.setObjective(profit - quicksum(fuel[i] + emission[i] + start_up[i] for i in self.t),
                           GRB.MAXIMIZE)

        model.update()

    def optimize(self):

        self.initialize_model(self.m, self.name)

        self.m.optimize()

        self.power = np.asarray([p.x for p in [x for x in self.m.getVars() if 'P_' in x.VarName]]).reshape((-1,))
        self.fuel = np.asarray([p.x for p in [x for x in self.m.getVars() if 'F_' in x.VarName]]).reshape((-1,))
        self.emission = np.asarray([p.x for p in [x for x in self.m.getVars() if 'E_' in x.VarName]]).reshape((-1,))
        self.start = np.asarray([p.x for p in [x for x in self.m.getVars() if 'S_' in x.VarName]]).reshape((-1,))
        self.generation['power' + self.power_plant['fuel'].capitalize()] = self.power

        return self.power


if __name__ == "__main__":

    pw = PowerPlant(t=np.arange(24), T=24, dt=1)

    power_price = 300 * np.ones(24)
    co = np.ones(24) * 23.8 * np.random.uniform(0.95, 1.05, 24)     # -- Emission Price     [€/t]
    gas = np.ones(24) * 24.8 * np.random.uniform(0.95, 1.05, 24)    # -- Gas Price          [€/MWh]
    lignite = 1.5 * np.random.uniform(0.95, 1.05)                   # -- Lignite Price      [€/MWh]
    coal = 9.9 * np.random.uniform(0.95, 1.05)                      # -- Hard Coal Price    [€/MWh]
    nuc = 1.0 * np.random.uniform(0.95, 1.05)                       # -- nuclear Price      [€/MWh]

    prices = dict(power=power_price, gas=gas, co=co, lignite=lignite, coal=coal, nuc=nuc)

    pw.set_parameter(date='2018-01-01', weather=None,
                     prices=prices)

    x = pw.optimize()
