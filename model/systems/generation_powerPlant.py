# third party modules
import os
import gurobipy as gby
import numpy as np


# model modules
from systems.basic_system import EnergySystem
os.chdir(os.path.dirname(os.path.dirname(__file__)))


class PowerPlant(EnergySystem):

    def __init__(self, T, steps, unitID, fuel, maxPower, minPower, eta, P0, chi, stopTime, runTime, gradP, gradM, on, off,
                 startCost, *args, **kwargs):
        super().__init__(T)

        # initialize default power plant
        self.power_plant =dict(fuel=fuel, maxPower=maxPower, minPower=minPower, eta=eta/100, P0=P0, chi=chi,
                               stopTime=stopTime, runTime=runTime, gradP=gradP, gradM=gradM, on=on, off=off)
        self.name = unitID  # power plant block name

        # initialize gurobi model for optimization
        self.m = gby.Model('power_plant')
        self.m.Params.OutputFlag = 0
        self.m.Params.TimeLimit = 30
        self.m.Params.MIPGap = 0.05
        self.m.__len__ = 1

        self.start_cost = startCost

        self.power, self. emission, self.fuel, self.start = np.zeros((self.T,), float), \
                                                            np.zeros((self.T,), float), \
                                                            np.zeros((self.T,), float), \
                                                            np.zeros((self.T,), float)

        self.data_storage = {offset: dict(power=np.zeros(min(self.T*2,48), float),
                                          emission=np.zeros(min(self.T*2,48), float),
                                          fuel=np.zeros(min(self.T*2,48), float),
                                          start=np.zeros(min(self.T*2,48), float),
                                          obj=0) for offset in steps}

    def initialize_model(self, m):

        delta = self.power_plant['maxPower'] - self.power_plant['minPower']
        su = self.power_plant['minPower']
        sd = self.power_plant['minPower']

        p_out = m.addVars(self.t, vtype=gby.GRB.CONTINUOUS, name='P_' + self.name, lb=0, ub=self.power_plant['maxPower'])
        # power at each time t
        m.addConstr(p_out[0] <= self.power_plant['P0'] + self.power_plant['gradP'])
        m.addConstr(p_out[0] >= self.power_plant['P0'] - self.power_plant['gradM'])
        # power corresponding to the optimization
        p_opt = m.addVars(self.t, vtype=gby.GRB.CONTINUOUS, name='opt_', lb=0, ub=delta)

        # states (on, ramp up, ramp down)
        z = m.addVars(self.t, vtype=gby.GRB.BINARY, name='z_' + self.name)
        v = m.addVars(self.t, vtype=gby.GRB.BINARY, name='v_' + self.name)
        w = m.addVars(self.t, vtype=gby.GRB.BINARY, name='w_' + self.name)
        m.addConstrs(p_out[i] == p_opt[i] + z[i] * self.power_plant['minPower'] for i in self.t)

        # power limits
        m.addConstrs(0 <= p_opt[i] for i in self.t)
        m.addConstrs(p_opt[i] <= delta * z[i] for i in self.t)
        m.addConstrs(p_opt[i] <= delta * z[i] - (self.power_plant['maxPower'] - su) * v[i]
                     - (self.power_plant['maxPower'] - sd) * w[i + 1] for i in self.t[:-1])

        # power gradients
        m.addConstrs(p_opt[i] - p_opt[i - 1] <= self.power_plant['gradP'] * z[i - 1] for i in self.t[1:])
        m.addConstrs(p_opt[i - 1] - p_opt[i] <= self.power_plant['gradM'] * z[i] for i in self.t[1:])

        # run- & stop-times
        m.addConstrs(1 - z[i] >= gby.quicksum(w[k] for k in range(max(0, i + 1 - self.power_plant['stopTime']), i))
                     for i in self.t)
        m.addConstrs(z[i] >= gby.quicksum(v[k] for k in range(max(0, i + 1 - self.power_plant['runTime']), i))
                     for i in self.t)
        m.addConstrs(z[i - 1] - z[i] + v[i] - w[i] == 0 for i in self.t[1:])

        # initialize stat
        if self.power_plant['on'] > 0:
            m.addConstrs(z[i] == 1 for i in range(0, self.power_plant['runTime']
                                                  - self.power_plant['on']))
        else:
            m.addConstrs(z[i] == 0 for i in range(0, self.power_plant['stopTime']
                                                  - self.power_plant['off']))
        # fuel cost
        fuel = m.addVars(self.t, vtype=gby.GRB.CONTINUOUS, name='F_' + self.name, lb=-gby.GRB.INFINITY, ub=gby.GRB.INFINITY)
        if self.power_plant['fuel'] == 'lignite':
            m.addConstrs(fuel[i] == p_out[i] / self.power_plant['eta'] * self.prices['lignite'] for i in self.t)
        if self.power_plant['fuel'] == 'coal':
            m.addConstrs(fuel[i] == p_out[i] / self.power_plant['eta'] * self.prices['coal'] for i in self.t)
        if self.power_plant['fuel'] == 'gas':
            m.addConstrs(fuel[i] == p_out[i] / self.power_plant['eta'] * self.prices['gas'][i] for i in self.t)
        if self.power_plant['fuel'] == 'nuc':
            m.addConstrs(fuel[i] == p_out[i] / self.power_plant['eta'] * self.prices['nuc'] for i in self.t)

        # emission cost
        emission = m.addVars(self.t, vtype=gby.GRB.CONTINUOUS, name='E_' + self.name, lb=0, ub=gby.GRB.INFINITY)
        m.addConstrs(emission[i] == p_out[i] * self.power_plant['chi'] / self.power_plant['eta']
                     * self.prices['co'][i] for i in self.t)

        # start cost
        start_up = m.addVars(self.t, vtype=gby.GRB.CONTINUOUS, name='S_' + self.name, lb=0, ub=gby.GRB.INFINITY)
        m.addConstrs(start_up[i] == v[i] * self.start_cost for i in self.t)

        profit = m.addVar(vtype=gby.GRB.CONTINUOUS, name='Profit' + self.name, lb=-gby.GRB.INFINITY, ub=gby.GRB.INFINITY)
        m.addConstr(profit == gby.quicksum(p_out[i] * self.prices['power'][i] for i in self.t))
        m.setObjective(profit - gby.quicksum(fuel[i] + emission[i] + start_up[i] for i in self.t),
                       gby.GRB.MAXIMIZE)

        m.update()

    def optimize(self):

        self.initialize_model(self.m)

        self.m.optimize()

        self.power = np.asarray([p.x for p in [x for x in self.m.getVars() if 'P_' in x.VarName]]).reshape((-1,))
        self.fuel = np.asarray([p.x for p in [x for x in self.m.getVars() if 'F_' in x.VarName]]).reshape((-1,))
        self.emission = np.asarray([p.x for p in [x for x in self.m.getVars() if 'E_' in x.VarName]]).reshape((-1,))
        self.start = np.asarray([p.x for p in [x for x in self.m.getVars() if 'S_' in x.VarName]]).reshape((-1,))
        self.generation['power' + self.power_plant['fuel'].capitalize()] = self.power

        return self.power


if __name__ == "__main__":
    plant = {'unitID':'x',
             'fuel':'lignite',
             'maxPower':300,
             'minPower':100,
             'eta':0.4,
             'P0':120,
             'chi': 0.407,
             'stopTime':5,
             'runTime':5,
             'gradP':300,
             'gradM':300,
             'on':1,
             'off':0,
             'startCost':10*10**3}
    pw = PowerPlant(T=24, **plant)

    power_price = 4000 * np.ones(24)
    co = np.ones(24) * 23.8 * np.random.uniform(0.95, 1.05, 24)     # -- Emission Price     [€/t]
    gas = np.ones(24) * 24.8 * np.random.uniform(0.95, 1.05, 24)    # -- Gas Price          [€/MWh]
    lignite = 1.5 * np.random.uniform(0.95, 1.05)                   # -- Lignite Price      [€/MWh]
    coal = 9.9 * np.random.uniform(0.95, 1.05)                      # -- Hard Coal Price    [€/MWh]
    nuc = 1.0 * np.random.uniform(0.95, 1.05)                       # -- nuclear Price      [€/MWh]

    prices = dict(power=power_price, gas=gas, co=co, lignite=lignite, coal=coal, nuc=nuc)

    pw.set_parameter(date='2018-01-01', weather=None,
                     prices=prices)

    x = pw.optimize()
    print(x)