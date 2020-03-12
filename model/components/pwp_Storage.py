from gurobipy import *
from components.basic_EnergySystem import es_model

class storage_gurobi(es_model):

    def __init__(self, t, T, dt, model):
        super().__init__(t, T, dt)
        self.m = model

    def build(self, name, data, timeseries):

        # Leistung & Volumen & Zustand
        power = self.m.addVars(self.t, vtype=GRB.CONTINUOUS, name='P_' + name, lb=-GRB.INFINITY, ub=GRB.INFINITY)
        volume = self.m.addVars(self.t, vtype=GRB.CONTINUOUS, name='V_' + name, lb=data['VMin'], ub=data['VMax'])
        pP = self.m.addVars(self.t, vtype=GRB.CONTINUOUS, name='P+_' + name, lb=0, ub=data['P+_Max'])
        pM = self.m.addVars(self.t, vtype=GRB.CONTINUOUS, name='P-_' + name, lb=0, ub=data['P-_Max'])
        on = self.m.addVars(self.t, vtype=GRB.BINARY, name='On_' + name)

        # Erlöse
        profit = self.m.addVars(self.t, vtype=GRB.CONTINUOUS, name='Profit_' + name, lb=-GRB.INFINITY, ub=GRB.INFINITY)
        self.m.addConstrs(profit[i] == power[i] * timeseries['power'][i] for i in self.t)

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