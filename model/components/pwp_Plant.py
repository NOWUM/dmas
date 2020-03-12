from gurobipy import *
import numpy as np
from components.basic_EnergySystem import es_model

class powerPlant_gurobi(es_model):

    def __init__(self, t, T, dt, model):
        super().__init__(t, T, dt)
        self.m = model

    def build(self, name, data, timeseries):

        # leistung & Zustand
        power = self.m.addVars(self.t, vtype=GRB.CONTINUOUS, name='P_' + name, lb=0, ub=GRB.INFINITY)
        on = self.m.addVars(self.t, vtype=GRB.BINARY, name='On_' + name)
        gradP = self.m.addVars(self.t, vtype=GRB.CONTINUOUS, name='gradUp_' + name, lb=0, ub=GRB.INFINITY)
        gradM = self.m.addVars(self.t, vtype=GRB.CONTINUOUS, name='gradDown_' + name, lb=0, ub=GRB.INFINITY)

        # Erlöse
        profit = self.m.addVars(self.t, vtype=GRB.CONTINUOUS, name='Profit_' + name, lb=-GRB.INFINITY, ub=GRB.INFINITY)
        self.m.addConstrs(profit[i] == power[i] * timeseries['power'][i] for i in self.t)

        # Brennstoffkosten
        fuel = self.m.addVars(self.t, vtype=GRB.CONTINUOUS, name='F_' + name, lb=-GRB.INFINITY, ub=GRB.INFINITY)
        if data['fuel'] == 'lignite':
            self.m.addConstrs(fuel[i] == power[i] / data['eta'] * timeseries['lignite'] for i in self.t)
        if data['fuel'] == 'coal':
            self.m.addConstrs(fuel[i] == power[i] / data['eta'] * timeseries['coal'] for i in self.t)
        if data['fuel'] == 'gas':
            self.m.addConstrs(fuel[i] == power[i] / data['eta'] * timeseries['gas'][i] for i in self.t)
        if data['fuel'] == 'nuc':
            self.m.addConstrs(fuel[i] == power[i] / data['eta'] * timeseries['nuc'] for i in self.t)

        # Emissionskosten
        emission = self.m.addVars(self.t, vtype=GRB.CONTINUOUS, name='E_' + name, lb=0, ub=GRB.INFINITY)
        self.m.addConstrs(emission[i] == power[i] * data['chi'] * timeseries['co'][i] for i in self.t)

        # Berücksichtigung der Startbedingungen
        self.m.addConstr(power[0] <= data['P0'] + data['gradP'])
        self.m.addConstr(power[0] >= data['P0'] - data['gradM'])
        # Berücksichtigung der Gradienten
        self.m.addConstrs(power[i] <= power[i - 1] + data['gradP'] for i in self.t[1:])
        self.m.addConstrs(power[i] >= power[i - 1] - data['gradM'] for i in self.t[1:])
        # Wenn das Kraftwerk läuft --> [Pmin,Pmax]
        self.m.addConstrs(power[i] >= on[i] * data['powerMin'] for i in self.t)
        self.m.addConstrs(power[i] <= on[i] * data['powerMax'] for i in self.t)
        # Verfügbare Gradienten für Regelleistung
        self.m.addConstrs(gradP[i] == on[i] * min(data['gradP'], data['powerMax'] - data['P0']) for i in self.t)
        self.m.addConstrs(gradM[i] == on[i] * min(data['gradM'], data['P0'] - data['powerMin']) for i in self.t)

        self.m.update()