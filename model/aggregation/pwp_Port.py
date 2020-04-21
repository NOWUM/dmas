import numpy as np
from components.pwp_Plant import powerPlant_gurobi as power_plant
from components.pwp_Storage import storage_gurobi as storage
from aggregation.basic_Port import port_model
from gurobipy import *

class pwpPort(port_model):

    def __int__(self, T=24, dt=1, gurobi=True, date='2020-01-01', typ ='DEM'):
        super().__init__(T, dt, gurobi, date, typ)

    def addToPortfolio(self, name, energysystem):
        data = energysystem[name]

        # -- PWP
        if data['typ'] == 'konv':              # konv. power plant
            data.update(dict(model=power_plant(t=self.t, T=self.T, dt=self.dt, model=self.m)))
        elif data['typ'] == 'storage':           # storage plant
            data.update(dict(model=storage(t=self.t, T=self.T, dt=self.dt, model=self.m)))

        self.energySystems.update(energysystem)

    def buildModel(self, response=[], max_=False):
        # ----- remove all constrains and vars -----
        self.m.remove(self.m.getVars())
        self.m.remove(self.m.getConstrs())
        # ----- build up each system -----
        for key, data in self.energySystems.items():
            data['model'].build(key, data, self.prices)

        # ----- build up aggregation constrains and vars -----

        # ----- total power -----
        power = self.m.addVars(self.t, vtype=GRB.CONTINUOUS, name='P', lb=-GRB.INFINITY, ub=GRB.INFINITY)
        pPower = [x for x in self.m.getVars() if 'P_' in x.VarName]
        self.m.addConstrs(power[i] == quicksum(p for p in pPower if '[%i]' % i in p.VarName) for i in self.t)

        # ----- balancing power -----
        gradP = [x for x in self.m.getVars() if 'gradUp_' in x.VarName]
        self.m.addConstrs(
            float(self.posBalPower[i]) <= quicksum(x for x in gradP if '[%i]' % i in x.VarName) for i in self.t)

        gradM = [x for x in self.m.getVars() if 'gradDown_' in x.VarName]
        self.m.addConstrs(
            float(self.negBalPower[i]) <= quicksum(x for x in gradM if '[%i]' % i in x.VarName) for i in self.t)

        maxPower = np.asarray(self.pwpCap - self.posBalPower)
        self.m.addConstrs((power[i] <= maxPower[i] for i in self.t))
        self.m.addConstrs((power[i] >= float(self.negBalPower[i]) for i in self.t))
        # ----- fuel -----
        fuel = self.m.addVars(self.t, vtype=GRB.CONTINUOUS, name='F', lb=-GRB.INFINITY, ub=GRB.INFINITY)
        pFuel = [x for x in self.m.getVars() if 'F_' in x.VarName]
        self.m.addConstrs(fuel[i] == quicksum(f for f in pFuel if '[%i]' % i in f.VarName) for i in self.t)
        # ----- emissions -----
        emission = self.m.addVars(self.t, vtype=GRB.CONTINUOUS, name='E', lb=-GRB.INFINITY, ub=GRB.INFINITY)
        pEmssion = [x for x in self.m.getVars() if 'E_' in x.VarName]
        self.m.addConstrs(
            emission[i] == quicksum(e for e in pEmssion if '[%i]' % i in e.VarName) for i in self.t)
        # ----- cashflow -----
        profit = self.m.addVar(vtype=GRB.CONTINUOUS, name='Profit', lb=-GRB.INFINITY, ub=GRB.INFINITY)
        self.m.addConstr(profit == quicksum(power[i] * self.prices['power'][i] for i in self.t))

        if len(response) == 0:
            if max_:
                self.m.setObjective(quicksum(power[i] for i in self.t), GRB.MAXIMIZE)
            else:
                # objective function (max cashflow)
                self.m.setObjective(profit - quicksum(fuel[i] + emission[i] for i in self.t), GRB.MAXIMIZE)
        else:
            minus = self.m.addVars(self.t, vtype=GRB.CONTINUOUS, name='minus', lb=0, ub=GRB.INFINITY)
            plus = self.m.addVars(self.t, vtype=GRB.CONTINUOUS, name='plus', lb=0, ub=GRB.INFINITY)
            self.m.addConstrs((response[i] - power[i] == -minus[i] + plus[i]) for i in self.t)
            self.m.setObjective(quicksum(plus[i] + minus[i] for i in self.t), GRB.MINIMIZE)
        # ----- update model -----
        self.m.update()

    def optimize(self):
        power = np.zeros_like(self.t)
        try:
            self.m.optimize()
            power = np.asanyarray([self.m.getVarByName('P[%i]' % i).x for i in self.t], np.float)
            power = np.round(power, 2)
        except Exception as e:
            print(e)
        self.power = power
        return power

    def fixPlaning(self):
        power = np.zeros_like(self.t)
        try:
            self.m.optimize()
            for key, value in self.energySystems.items():
                if value['typ'] == 'konv':
                    value['P0'] = [np.round(self.m.getVarByName('P_%s[%i]' % (key, i)).x, 2) for i in self.t][-1]
                if value['typ'] == 'storage':
                    value['P+0'] = [self.m.getVarByName('P+_%s[%i]' % (key, i)).x for i in self.t][-1]
                    value['P-0'] = [self.m.getVarByName('P-_%s[%i]' % (key, i)).x for i in self.t][-1]
                    value['V0'] = [self.m.getVarByName('V_%s[%i]' % (key, i)).x for i in self.t][-1]
            power = np.asarray([self.m.getVarByName('P[%i]' % i).x for i in self.t], np.float)
        except Exception as e:
            print(e)
        self.power = power
        return power

if __name__ == "__main__":
    test = pwpPort()