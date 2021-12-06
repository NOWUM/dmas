# third party modules
import numpy as np
import gurobipy as gby

# model modules
from systems.storage_hydroPlant import Storage
from aggregation.portfolio import PortfolioModel


class StrPort(PortfolioModel):

    def __int__(self, T=24, date='2020-01-01'):
        super().__init__(T, date)

        self.m = gby.Model('aggregation')
        self.m.Params.OutputFlag = 0
        self.m.Params.TimeLimit = 30
        self.m.Params.MIPGap = 0.05
        self.m.__len__ = 1

        self.power = np.zeros((self.T,), np.float)
        self.volume = np.zeros((self.T,), np.float)

        self.fix = True

    def add_energy_system(self, energy_system):
        # build power plants
        energy_system.update(dict(model=Storage(name=energy_system['unitID'], storage=energy_system, T=self.T)))
        self.energy_systems.update({energy_system['unitID']: energy_system})
        self.capacities['storages'] += energy_system['VMax']

    def build_model(self, response=None):
        self.m.remove(self.m.getVars())
        self.m.remove(self.m.getConstrs())

        for key, value in self.energy_systems.items():
            value['model'].set_parameter(date=self.date, weather=self.weather, prices=self.prices)
            value['model'].initialize_model(self.m, key)
        self.m.update()

        # total power in portfolio
        power = self.m.addVars(self.t, vtype=gby.GRB.CONTINUOUS, name='P', lb=-gby.GRB.INFINITY, ub=gby.GRB.INFINITY)
        self.m.addConstrs(power[i] == gby.quicksum(p for p in [x for x in self.m.getVars() if 'P_' in x.VarName]
                                               if '[%i]' % i in p.VarName) for i in self.t)
        # total volume in portfolio
        volume = self.m.addVars(self.t, vtype=gby.GRB.CONTINUOUS, name='V', lb=0, ub=gby.GRB.INFINITY)
        self.m.addConstrs(volume[i] == gby.quicksum(v for v in [x for x in self.m.getVars() if 'V_' in x.VarName]
                                                if '[%i]' % i in v.VarName) for i in self.t)
        # total profit in portfolio
        profit = self.m.addVar(vtype=gby.GRB.CONTINUOUS, name='Profit', lb=-gby.GRB.INFINITY, ub=gby.GRB.INFINITY)
        self.m.addConstr(profit == gby.quicksum(power[i] * self.prices['power'][i] for i in self.t))

        self.m.update()
        if response is None:
            # objective function (max cashflow)
            self.m.setObjective(profit, gby.GRB.MAXIMIZE)
            self.fix = False
        else:
            self.fix = True
            delta_power = self.m.addVars(self.t, vtype=gby.GRB.CONTINUOUS, name='ReBA', lb=0, ub=gby.GRB.INFINITY)
            minus = self.m.addVars(self.t, vtype=gby.GRB.CONTINUOUS, name='minus', lb=0, ub=gby.GRB.INFINITY)
            plus = self.m.addVars(self.t, vtype=gby.GRB.CONTINUOUS, name='plus', lb=0, ub=gby.GRB.INFINITY)
            self.m.addConstrs((response[i] - power[i] == -minus[i] + plus[i]) for i in self.t)
            self.m.addConstrs(minus[i] + plus[i] == delta_power[i] for i in self.t)
            self.m.setObjective(gby.quicksum(delta_power[i] for i in self.t), gby.GRB.MINIMIZE)

        self.m.update()

    def optimize(self):

        self.power = np.zeros((self.T,), np.float)
        self.volume = np.zeros((self.T,), np.float)

        # initialize dict for fuel sum calculation
        self.generation = dict(water=np.zeros((self.T,), np.float))

        self.m.optimize()
        power = np.asarray([self.m.getVarByName('P[%i]' % i).x for i in self.t], np.float).reshape((-1,))
        self.power = np.round(power, 2)
        # total emissions costs [€] for each hour
        volume = np.asarray([self.m.getVarByName('V[%i]' % i).x for i in self.t], np.float).reshape((-1,))
        self.volume = np.round(volume, 2)

        for key, value in self.energy_systems.items():
            # set output power for each energy system (storage)
            value['model'].power = np.asarray([self.m.getVarByName('P_%s[%i]' % (key, i)).x
                                               for i in self.t], np.float).reshape((-1,))
            value['model'].volume = np.asarray([self.m.getVarByName('V_%s[%i]' % (key, i)).x
                                                for i in self.t], np.float).reshape((-1,))

            self.generation['power%s' % value['fuel'].capitalize()] += value['model'].power

            if self.fix:
                value['model'].storage['P+0'] = self.m.getVarByName('P+_%s[%i]' % (key, 23)).x
                value['model'].storage['P-0'] = self.m.getVarByName('P-_%s[%i]' % (key, 23)).x
                value['model'].storage['V0'] = self.m.getVarByName('V_%s[%i]' % (key, 23)).x
                value['model'].power = [self.m.getVarByName('P_%s[%i]' % (key, i)).x for i in self.t]
                value['model'].volume = [self.m.getVarByName('V_%s[%i]' % (key, i)).x for i in self.t]

        self.generation['powerTotal'] = power

        return self.power, self.volume