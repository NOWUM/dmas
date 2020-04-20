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
        power = self.m.addVars(self.t, vtype=GRB.CONTINUOUS, name='P_' + name, lb=0, ub=GRB.INFINITY)
        # Leistung zur Lösung des Optimierungsproblems
        p = self.m.addVars(self.t, vtype=GRB.CONTINUOUS, lb=0, ub=GRB.INFINITY, name='p_')

        # Zustände des Kraftwerks
        v = self.m.addVars(self.t, vtype=GRB.BINARY, name='v')      # von 0 -> Pmin
        w = self.m.addVars(self.t, vtype=GRB.BINARY, name='w')      # von Pmin -> 0
        z = self.m.addVars(self.t, vtype=GRB.BINARY, name='z')      # Normalbetrieb

        # Gradienten für Regelleistung
        gradP = self.m.addVars(self.t, vtype=GRB.CONTINUOUS, name='gradUp_' + name, lb=0, ub=GRB.INFINITY)
        gradM = self.m.addVars(self.t, vtype=GRB.CONTINUOUS, name='gradDown_' + name, lb=0, ub=GRB.INFINITY)

        # Startbedingungen der Zustände
        self.m.addConstr(z[0] == 0)
        self.m.addConstr(v[0] == 0)
        self.m.addConstr(w[0] == 0)

        # Verknüpfung der Zustände über Laufzeitbedingungen
        self.m.addConstrs(z[i - 1] - z[i] + v[i] - w[i] == 0 for i in self.t[1:])
        self.m.addConstrs(1-z[i] >= quicksum(w[k] for k in range(min(1, i+1-int(data['stopTime']/self.dt),i))) for i in self.t[1:])
        self.m.addConstrs(z[i] >= quicksum(v[k] for k in range(min(1, i+1-int(data['runTime']/self.dt),i))) for i in self.t[1:])

        # Startbedingungen der lesitung
        self.m.addConstr(p[0] == data['P_0'] - data['powerMin'])
        # Verknüpfung der Leistungen zur Lösung & der tatsächlichen leistung
        self.m.addConstrs(power[i] == data['powerMin'] * z[i] + p[i] for i in self.t)

        # Leitungsgrenzen
        self.m.addConstrs(p[i] <= (data['powerMax'] - data['powerMin']) * z[i] - (data['powerMax'] - data['gradP']) * v[i] for i in self.t)
        self.m.addConstrs(p[i] <= (data['powerMax'] - data['powerMin']) * z[i] - (data['powerMax'] - data['gradM']) * w[i] for i in self.t)

        # Flex-Grenzen
        self.m.addConstrs(p[i] - p[i-1] <= data['gradP'] * z[i] + (data['gradP'] - data['powerMin']) * v[i] for i in self.t[1:])
        self.m.addConstrs(p[i-1] - p[i] <= data['gradM'] * z[i] + (data['gradM'] - data['powerMin']) * v[i] for i in self.t[1:])

        # Erlöse
        profit = self.m.addVars(self.t, vtype=GRB.CONTINUOUS, name='Profit_' + name, lb=-GRB.INFINITY, ub=GRB.INFINITY)
        self.m.addConstrs(profit[i] == power[i] * ts['power'][i] for i in self.t)

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

        # Emissionskosten
        emission = self.m.addVars(self.t, vtype=GRB.CONTINUOUS, name='E_' + name, lb=0, ub=GRB.INFINITY)
        self.m.addConstrs(emission[i] == power[i] * data['chi'] * ts['co'][i] for i in self.t)

        # Wenn das Kraftwerk läuft --> [Pmin,Pmax]
        self.m.addConstrs(power[i] >= z[i] * data['powerMin'] for i in self.t)
        self.m.addConstrs(power[i] <= z[i] * data['powerMax'] for i in self.t)
        # Verfügbare Gradienten für Regelleistung
        self.m.addConstrs(gradP[i] == z[i] * min(data['gradP'], data['powerMax'] - data['P0']) for i in self.t)
        self.m.addConstrs(gradM[i] == z[i] * min(data['gradM'], data['P0'] - data['powerMin']) for i in self.t)

        self.m.update()