# third party modules
import numpy as np
import gurobipy as gby
import copy


# model modules
from components.generation_powerPlant import PowerPlant
from aggregation.portfolio import PortfolioModel


class PwpPort(PortfolioModel):

    def __int__(self, T=24, dt=1, gurobi=True, date='2020-01-01'):
        super().__init__(T, dt, date)

        self.m = gby.Model('aggregation')
        self.m.Params.OutputFlag = 0
        self.m.Params.TimeLimit = 30
        self.m.Params.MIPGap = 0.05
        self.m.__len__ = 1

        self.power = np.zeros_like(self.t, np.float)
        self.fuel = np.zeros_like(self.t, np.float)
        self.start = np.zeros_like(self.t, np.float)
        self.emission = np.zeros_like(self.t, np.float)

        self.fix = True

    def add_energy_system(self, name, energy_system):
        # print(energy_system)
        data = copy.deepcopy(energy_system[name])
        # build power plants
        data.update(dict(model=PowerPlant(name=name, power_plant=copy.deepcopy(data),
                                          T=self.T, t=np.arange(self.T), dt=self.dt)))

        self.energy_systems.update({name: data})

    def build_model(self, response=None, max_power=False):
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
        # total fuel cost in portfolio
        fuel = self.m.addVars(self.t, vtype=gby.GRB.CONTINUOUS, name='F', lb=-gby.GRB.INFINITY, ub=gby.GRB.INFINITY)
        self.m.addConstrs(fuel[i] == gby.quicksum(f for f in [x for x in self.m.getVars() if 'F_' in x.VarName]
                                              if '[%i]' % i in f.VarName) for i in self.t)
        # total emission cost in portfolio
        emission = self.m.addVars(self.t, vtype=gby.GRB.CONTINUOUS, name='E', lb=-gby.GRB.INFINITY, ub=gby.GRB.INFINITY)
        self.m.addConstrs(emission[i] == gby.quicksum(e for e in [x for x in self.m.getVars() if 'E_' in x.VarName]
                                                  if '[%i]' % i in e.VarName) for i in self.t)
        # total start cost in portfolio
        start = self.m.addVars(self.t, vtype=gby.GRB.CONTINUOUS, name='S', lb=-gby.GRB.INFINITY, ub=gby.GRB.INFINITY)
        self.m.addConstrs(start[i] == gby.quicksum(s for s in [x for x in self.m.getVars() if 'S_' in x.VarName]
                                               if '[%i]' % i in s.VarName) for i in self.t)
        # total profit in portfolio
        profit = self.m.addVar(vtype=gby.GRB.CONTINUOUS, name='Profit', lb=-gby.GRB.INFINITY, ub=gby.GRB.INFINITY)
        self.m.addConstr(profit == gby.quicksum(power[i] * self.prices['power'][i] for i in self.t))

        self.m.update()
        if response is None:
            if max_power:
                self.m.setObjective(gby.quicksum(power[i] for i in self.t), gby.GRB.MAXIMIZE)
            else:
                # objective function (max cash_flow)
                self.m.setObjective(profit - gby.quicksum(fuel[i] + emission[i] + start[i] for i in self.t),
                                    gby.GRB.MAXIMIZE)
            self.fix = False
        else:
            self.fix = True
            delta_power = self.m.addVars(self.t, vtype=gby.GRB.CONTINUOUS, name='delta_power', lb=0, ub=gby.GRB.INFINITY)
            minus = self.m.addVars(self.t, vtype=gby.GRB.CONTINUOUS, name='minus', lb=0, ub=gby.GRB.INFINITY)
            plus = self.m.addVars(self.t, vtype=gby.GRB.CONTINUOUS, name='plus', lb=0, ub=gby.GRB.INFINITY)
            self.m.addConstrs(minus[i] + plus[i] == delta_power[i] for i in self.t)
            self.m.addConstrs((response[i] - power[i] == -minus[i] + plus[i]) for i in self.t)

            delta_costs = self.m.addVars(self.t, vtype=gby.GRB.CONTINUOUS, name='delta_costs', lb=0, ub=gby.GRB.INFINITY)
            self.m.addConstrs(delta_costs[i] == delta_power[i] * np.abs(self.prices['power'][i]) for i in self.t)
            self.m.setObjective(profit - gby.quicksum(fuel[i] + emission[i] + start[i] + delta_costs[i] for i in self.t),
                                gby.GRB.MAXIMIZE)
        self.m.update()

    def optimize(self):

        self.power = np.zeros_like(self.t, np.float)
        self.emission = np.zeros_like(self.t, np.float)
        self.fuel = np.zeros_like(self.t, np.float)
        self.start = np.zeros_like(self.t, np.float)

        # initialize dict for fuel sum calculation
        self.generation = dict(powerLignite=np.zeros_like(self.t, np.float),     # total generation lignite   [MW]
                               powerCoal=np.zeros_like(self.t, np.float),        # total generation caol      [MW]
                               powerGas=np.zeros_like(self.t, np.float),         # total generation gas       [MW]
                               powerNuc=np.zeros_like(self.t, np.float))         # total generation nuc       [MW])

        self.m.optimize()

        try:
            power = np.asarray([self.m.getVarByName('P[%i]' % i).x for i in self.t], np.float).reshape((-1,))
            self.power = np.round(power, 2)
            # total emissions costs [€] for each hour
            emission = np.asarray([self.m.getVarByName('E[%i]' % i).x for i in self.t], np.float).reshape((-1,))
            self.emission = np.round(emission, 2)
            # total fuel costs [€] for each hour
            fuel = np.asarray([self.m.getVarByName('F[%i]' % i).x for i in self.t], np.float).reshape((-1,))
            self.fuel = np.round(fuel, 2)
            # total start costs [€] for each hour
            start = np.asarray([self.m.getVarByName('S[%i]' % i).x for i in self.t], np.float).reshape((-1,))
            self.start = np.round(start, 2)

            for key, value in self.energy_systems.items():
                # set output power for each energy system (power plant)
                value['model'].power = np.asarray([self.m.getVarByName('P_%s[%i]' % (key, i)).x
                                                   for i in self.t], np.float).reshape((-1,))
                value['model'].emission = np.asarray([self.m.getVarByName('E_%s[%i]' % (key, i)).x
                                                      for i in self.t], np.float).reshape((-1,))
                value['model'].fuel = np.asarray([self.m.getVarByName('F_%s[%i]' % (key, i)).x
                                                  for i in self.t], np.float).reshape((-1,))
                value['model'].start = np.asarray([self.m.getVarByName('S_%s[%i]' % (key, i)).x
                                                  for i in self.t], np.float).reshape((-1,))

                self.generation['power%s' % value['fuel'].capitalize()] += value['model'].power

                if self.fix:
                    value['model'].power_plant['P0'] = self.m.getVarByName('P_%s[%i]' % (key, 23)).x
                    z = np.asanyarray([self.m.getVarByName('z_%s[%i]' % (key, i)).x for i in self.t[:24]], np.float)
                    if z[-1] > 0:
                        index = -1 * value['model'].power_plant['runTime']
                        value['model'].power_plant['on'] = np.count_nonzero(z[index:])
                        value['model'].power_plant['off'] = 0
                    else:
                        index = -1 * value['model'].power_plant['stopTime']
                        value['model'].power_plant['off'] = np.count_nonzero(1 - z[index:])
                        value['model'].power_plant['on'] = 0

            self.generation['powerTotal'] = power

        except Exception as e:
            print(e)

        return self.power, self.emission, self.fuel, self.start


if __name__ == "__main__":
    pass
