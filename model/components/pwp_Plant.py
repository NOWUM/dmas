from gurobipy import *
import numpy as np
from components.basic_EnergySystem import energySystem as es

class powerPlant_gurobi(es):

    def __init__(self,
                 model,                                     # Gurobi Model
                 t=np.arange(24), T=24, dt=1):              # Metainfo Zeitt, T, dt
        super().__init__(t, T, dt)
        self.m = model

    def build(self, name, data, ts):

        # Leistung des Kraftwerkes zu jedem Zeitschritt t
        power = self.m.addVars(self.t, vtype=GRB.CONTINUOUS, name='P_' + name, lb=0, ub=data['powerMax'])
        on = self.m.addVars(self.t, vtype=GRB.BINARY, name='On_' + name)

        # Berücksichtigung der Startbedingungen
        if data['P0'] == 0:
            self.m.addConstr(power[0] == on[0] * data['powerMin'])
        else:
            self.m.addConstr(power[0] <= data['P0'] + data['gradP'])
            self.m.addConstr(power[0] >= data['P0'] - data['gradM'])

        # Berücksichtigung der Gradienten
        self.m.addConstrs(power[i] <= power[i - 1] + data['gradP'] for i in self.t[1:])
        self.m.addConstrs(power[i] >= power[i - 1] - data['gradM'] - (1-on[i]) * (data['powerMin']+1) for i in self.t[1:])
        # Wenn das Kraftwerk läuft --> [Pmin,Pmax]
        self.m.addConstrs(power[i] >= on[i] * data['powerMin'] for i in self.t[1:])
        self.m.addConstrs(power[i] <= on[i] * data['powerMax'] for i in self.t[1:])

        # Brennstoffkosten
        fuel = self.m.addVars(self.t, vtype=GRB.CONTINUOUS, name='F_' + name, lb=-GRB.INFINITY, ub=GRB.INFINITY)
        if data['fuel'] == 'lignite':
            self.m.addConstrs(fuel[i] == power[i] / data['eta'] * ts['lignite'] for i in self.t)
        if data['fuel'] == 'coal':
            self.m.addConstrs(fuel[i] == power[i] / data['eta'] * ts['coal'] for i in self.t)
        if data['fuel'] == 'gas':
            self.m.addConstrs(fuel[i] == power[i] / data['eta'] * ts['gas'][i] for i in self.t)
        if data['fuel'] == 'nuc':
            self.m.addConstrs(fuel[i] == power[i] / data['eta'] * ts['nuc'] for i in self.t)

        # CO2 Emissionskosten
        emission = self.m.addVars(self.t, vtype=GRB.CONTINUOUS, name='E_' + name, lb=0, ub=GRB.INFINITY)
        self.m.addConstrs(emission[i] == power[i] * data['chi']/ data['eta'] * ts['co'][i] for i in self.t)

        self.m.update()
