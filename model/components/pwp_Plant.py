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

        if data['fuel'] == 'gas':
            start_cost = 24 * data['maxPower']
        if data['fuel'] == 'lignite':
            if data['maxPower'] > 500:
                start_cost = 50 * data['maxPower']
            else:
                start_cost = 105 * data['maxPower']
        if data['fuel'] == 'coal':
            if data['maxPower'] > 500:
                start_cost = 50 * data['maxPower']
            else:
                start_cost = 105 * data['maxPower']
        if data['fuel'] == 'nuc':
            start_cost = 50 * data['maxPower']

        delta = data['maxPower'] - data['minPower']
        su = data['minPower']
        sd = data['minPower']

        # Leistung des Kraftwerkes zu jedem Zeitschritt t
        p_out = self.m.addVars(self.t, vtype=GRB.CONTINUOUS, name='P_' + name, lb=0, ub=data['maxPower'])
        self.m.addConstr(p_out[0] <= data['P0'] + data['gradP'])
        self.m.addConstr(p_out[0] >= data['P0'] - data['gradM'])
        # Leistung für die Optimierung
        p_opt = self.m.addVars(self.t, vtype=GRB.CONTINUOUS, name='opt_' + name, lb=0, ub=delta)

        # Zustände des Kraftwerks
        z = self.m.addVars(self.t, vtype=GRB.BINARY, name='z_' + name)          # Kraftwerk in Betrieb
        v = self.m.addVars(self.t, vtype=GRB.BINARY, name='v_' + name)          # von 0 zu Pmin
        w = self.m.addVars(self.t, vtype=GRB.BINARY, name='w_' + name)          # von Pmin zu 0

        # Zusamamenhang zwischen opt_Leistung und der tatsächlichen Leistung
        self.m.addConstrs(p_out[i] == p_opt[i] + z[i] * data['minPower'] for i in self.t)

        # Leistungsgrenzen
        self.m.addConstrs(0 <= p_opt[i] for i in self.t)
        self.m.addConstrs(p_opt[i] <= delta * z[i] for i in self.t)
        self.m.addConstrs(p_opt[i] <= delta * z[i] - (data['maxPower'] - su) * v[i] - (data['maxPower'] - sd) * w[i+1]
                          for i in self.t[:-1])

        #Leistungsgradienten
        self.m.addConstrs(p_opt[i] - p_opt[i-1] <= data['gradP'] * z[i-1] for i in self.t[1:])
        self.m.addConstrs(p_opt[i-1] - p_opt[i] <= data['gradM'] * z[i] for i in self.t[1:])

        # Lauf- und Stillstandszeiten
        self.m.addConstrs(1-z[i] >= quicksum(w[k] for k in range(max(0, i+1 - data['stopTime']), i)) for i in self.t)
        self.m.addConstrs(z[i] >= quicksum(v[k] for k in range(max(0, i+1 - data['runTime']), i)) for i in self.t)
        self.m.addConstrs(z[i - 1] - z[i] + v[i] - w[i] == 0 for i in self.t[1:])

        # Startbedingungen (Kraftwerk an oder aus)
        if data['on'] > 0:
            self.m.addConstrs(z[i] == 1 for i in range(0, data['runTime'] - data['on']))
        else:
            self.m.addConstrs(z[i] == 0 for i in range(0, data['stopTime'] - data['off']))

        # Brennstoffkosten
        fuel = self.m.addVars(self.t, vtype=GRB.CONTINUOUS, name='F_' + name, lb=-GRB.INFINITY, ub=GRB.INFINITY)
        if data['fuel'] == 'lignite':
            self.m.addConstrs(fuel[i] == p_out[i] / data['eta'] * ts['lignite'] for i in self.t)
        if data['fuel'] == 'coal':
            self.m.addConstrs(fuel[i] == p_out[i] / data['eta'] * ts['coal'] for i in self.t)
        if data['fuel'] == 'gas':
            self.m.addConstrs(fuel[i] == p_out[i] / data['eta'] * ts['gas'][i] for i in self.t)
        if data['fuel'] == 'nuc':
            self.m.addConstrs(fuel[i] == p_out[i] / data['eta'] * ts['nuc'] for i in self.t)

        # CO2 Emissionskosten
        emission = self.m.addVars(self.t, vtype=GRB.CONTINUOUS, name='E_' + name, lb=0, ub=GRB.INFINITY)
        self.m.addConstrs(emission[i] == p_out[i] * data['chi'] / data['eta'] * ts['co'][i] for i in self.t)

        # Startkosten
        start_up = self.m.addVars(self.t, vtype=GRB.CONTINUOUS, name='S_' + name, lb=0, ub=GRB.INFINITY)
        self.m.addConstrs(start_up[i] == v[i] * start_cost for i in self.t)

        self.m.update()

