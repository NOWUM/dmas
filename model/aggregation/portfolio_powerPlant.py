# third party modules
import os
import numpy as np
import gurobipy as gby
import logging
import time

# model modules
from systems.generation_powerPlant import PowerPlant
from aggregation.portfolio import PortfolioModel

log = logging.getLogger('power_plant_portfolio')
log.setLevel('INFO')

class PwpPort(PortfolioModel):

    def __init__(self, steps, T=24, date='2020-01-01'):
        super().__init__(T, date)

        env = gby.Env(empty=True)
        env.setParam("ComputeServer", os.getenv('COMPUTE_SERVER'))
        env.start()

        self.m = gby.Model('aggregation', env=env)
        self.m.Params.OutputFlag = 0
        self.m.Params.TimeLimit = 30
        self.m.Params.MIPGap = 0.05
        self.m.__len__ = 1

        self.power = np.zeros((self.T,), float)
        self.fuel = np.zeros((self.T,), float)
        self.start = np.zeros((self.T,), float)
        self.emission = np.zeros((self.T,), float)

        self.steps = steps

        self.lock_generation = True

    def add_energy_system(self, energy_system):
        model=PowerPlant(T=self.T, steps=self.steps, **energy_system)
        key = str(energy_system['fuel']).replace('_combined', '')
        self.capacities[key] += energy_system['maxPower']
        self.energy_systems.append(model)

    def build_model(self, response=None, max_power=False):
        self.m.remove(self.m.getVars())
        self.m.remove(self.m.getConstrs())

        for model in self.energy_systems:
            model.set_parameter(date=self.date, weather=self.weather, prices=self.prices)
            model.initialize_model(self.m)
        self.m.update()

        # total power in portfolio
        power = self.m.addVars(self.t, vtype=gby.GRB.CONTINUOUS, name='P', lb=-gby.GRB.INFINITY, ub=gby.GRB.INFINITY)
        self.m.addConstrs(power[i] == gby.quicksum(p for p in [x for x in self.m.getVars() if 'P_' in x.VarName]
                                               if '[%i]' % i in p.VarName) for i in self.t)
        # total fuel cost in portfolio
        fuel = self.m.addVars(self.t, vtype=gby.GRB.CONTINUOUS, name='F', lb=0, ub=gby.GRB.INFINITY)
        self.m.addConstrs(fuel[i] == gby.quicksum(f for f in [x for x in self.m.getVars() if 'F_' in x.VarName]
                                              if '[%i]' % i in f.VarName) for i in self.t)
        # total emission cost in portfolio
        emission = self.m.addVars(self.t, vtype=gby.GRB.CONTINUOUS, name='E', lb=0, ub=gby.GRB.INFINITY)
        self.m.addConstrs(emission[i] == gby.quicksum(e for e in [x for x in self.m.getVars() if 'E_' in x.VarName]
                                                  if '[%i]' % i in e.VarName) for i in self.t)
        # total start cost in portfolio
        start = self.m.addVars(self.t, vtype=gby.GRB.CONTINUOUS, name='S', lb=0, ub=gby.GRB.INFINITY)
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
                self.m.setObjective(profit - gby.quicksum(start[i] + emission[i] + fuel[i] for i in self.t),
                                    gby.GRB.MAXIMIZE)
            self.lock_generation = False
        else:
            self.lock_generation = True
            delta_power = self.m.addVars(self.t, vtype=gby.GRB.CONTINUOUS, name='delta_power', lb=0, ub=gby.GRB.INFINITY)
            minus = self.m.addVars(self.t, vtype=gby.GRB.CONTINUOUS, name='minus', lb=0, ub=gby.GRB.INFINITY)
            plus = self.m.addVars(self.t, vtype=gby.GRB.CONTINUOUS, name='plus', lb=0, ub=gby.GRB.INFINITY)
            self.m.addConstrs(minus[i] + plus[i] == delta_power[i] for i in self.t)
            self.m.addConstrs(response[i] - power[i] == -minus[i] + plus[i] for i in self.t)

            delta_costs = self.m.addVars(self.t, vtype=gby.GRB.CONTINUOUS, name='delta_costs', lb=0, ub=gby.GRB.INFINITY)
            self.m.addConstrs(delta_costs[i] == delta_power[i] * np.abs(self.prices['power'][i]) for i in self.t)
            self.m.setObjective(profit - gby.quicksum(fuel[i] + emission[i] + start[i] + delta_costs[i] for i in self.t),
                                gby.GRB.MAXIMIZE)
        self.m.update()

    def optimize(self):

        self.reset_data()

        t = time.time()
        self.m.optimize()
        # log.info(f'optimize took {time.time() - t}')

        t = time.time()
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

        for model in self.energy_systems:
            model.power = np.asarray([self.m.getVarByName(f'P_{model.name}[{i}]').x for i in self.t]).reshape((-1,))
            model.emission = np.asarray([self.m.getVarByName(f'E_{model.name}[{i}]').x for i in self.t]).reshape((-1,))
            model.start = np.asarray([self.m.getVarByName(f'S_{model.name}[{i}]').x for i in self.t]).reshape((-1,))
            model.fuel = np.asarray([self.m.getVarByName(f'F_{model.name}[{i}]').x for i in self.t]).reshape((-1,))
            self.generation[f'{model.power_plant["fuel"].replace("_combined","")}'] += model.power

            if self.lock_generation:
                model.power_plant['P0'] = self.m.getVarByName(f'P_{model.name}[{23}]').x
                z = np.asarray([self.m.getVarByName(f'z_{model.name}[{i}]').x for i in self.t[:24]]).reshape((-1,))
                if z[-1] > 0:
                    index = -1 * model.power_plant['runTime']
                    model.power_plant['on'] = np.count_nonzero(z[index:])
                    model.power_plant['off'] = 0
                else:
                    index = -1 * model.power_plant['stopTime']
                    model.power_plant['off'] = np.count_nonzero(1 - z[index:])
                    model.power_plant['on'] = 0

        self.generation['powerTotal'] = power
        # log.info(f'append took {time.time() - t}')

        return self.power
