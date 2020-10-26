from gurobipy import *
import numpy as np
from components.energy_system import EnergySystem as es

class storage_gurobi(es):

    def __init__(self,
                 model,                                     # Gurobi Model
                 t=np.arange(24), T=24, dt=1):              # Metainfo Zeit t, T, dt
        super().__init__(t, T, dt)
        self.m = model

    def build(self, name, data, ts):

        # Leistung & Volumen & Zustand
        power = self.m.addVars(self.t, vtype=GRB.CONTINUOUS, name='P_' + name, lb=-GRB.INFINITY, ub=GRB.INFINITY)
        volume = self.m.addVars(self.t, vtype=GRB.CONTINUOUS, name='V_' + name, lb=data['VMin'], ub=data['VMax'])

        emission = self.m.addVars(self.t, vtype=GRB.CONTINUOUS, name='E_' + name, lb=0, ub=GRB.INFINITY)
        self.m.addConstrs(emission[i] == 0 for i in self.t)
        fuel = self.m.addVars(self.t, vtype=GRB.CONTINUOUS, name='F_' + name, lb=-GRB.INFINITY, ub=GRB.INFINITY)
        self.m.addConstrs(fuel[i] == 0 for i in self.t)
        start_up = self.m.addVars(self.t, vtype=GRB.CONTINUOUS, name='S_' + name, lb=0, ub=GRB.INFINITY)
        self.m.addConstrs(start_up[i] == 0 for i in self.t)

        pP = self.m.addVars(self.t, vtype=GRB.CONTINUOUS, name='P+_' + name, lb=0, ub=data['P+_Max'])
        pM = self.m.addVars(self.t, vtype=GRB.CONTINUOUS, name='P-_' + name, lb=0, ub=data['P-_Max'])
        on = self.m.addVars(self.t, vtype=GRB.BINARY, name='On_' + name)

        # Erlöse
        profit = self.m.addVars(self.t, vtype=GRB.CONTINUOUS, name='Profit_' + name, lb=-GRB.INFINITY, ub=GRB.INFINITY)
        self.m.addConstrs(profit[i] == power[i] * ts['power'][i] for i in self.t)

        # Leistung Speicher
        self.m.addConstrs(power[i] == -pP[i] + pM[i] for i in self.t)
        # Leistungsgrenzen
        self.m.addConstrs(pP[i] <= on[i] * data['P+_Max'] for i in self.t)
        self.m.addConstrs(pP[i] >= on[i] * data['P+_Min'] for i in self.t)
        self.m.addConstrs(pM[i] <= (1 - on[i]) * data['P-_Max'] for i in self.t)
        self.m.addConstrs(pM[i] >= (1 - on[i]) * data['P-_Min'] for i in self.t)

        # Füllstand
        self.m.addConstr(volume[0] == data['V0'] + self.dt * (data['eta+'] * pP[0] - pM[0] / data['eta-']))
        self.m.addConstrs(volume[i] == volume[i-1] + self.dt * (data['eta+'] * pP[i] - pM[i] / data['eta-']) for i in self.t[1:])

        self.m.update()