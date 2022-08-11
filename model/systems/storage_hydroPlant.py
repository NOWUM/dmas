# third party modules
import numpy as np
from pyomo.environ import Constraint, Var, Objective, SolverFactory, ConcreteModel, \
    Reals, Binary, maximize, quicksum, ConstraintList

# model modules
from systems.basic_system import EnergySystem


class Storage(EnergySystem):

    def __init__(self, T, unitID, eta_plus, eta_minus, fuel, V0, VMin, VMax, PPlusMax, PMinusMax, PPlusMin, PMinusMin):
        super().__init__(T)

        self.name = unitID

        storage = {"eta+": eta_plus, "eta-": eta_minus, "fuel": "water", "V0": V0, "VMin": VMin, "VMax": VMax,
                   "P+_Max": PPlusMax, "P-_Max": PMinusMax,"P+_Min": PPlusMin, "P-_Min": PMinusMin}
        self.storage = storage

        self.model = ConcreteModel()
        self.opt = SolverFactory('glpk')

        self.volume = np.zeros((self.T,), np.float)

        self.committed_power = None

    def build_model(self):

        self.model.clear()

        self.model.p_out = Var(self.t, within=Reals)
        self.model.p_plus = Var(self.t, bounds=(self.storage['P+_Min'], self.storage['P+_Max']), within=Reals)
        self.model.p_minus = Var(self.t, bounds=(self.storage['P-_Min'], self.storage['P-_Max']), within=Reals)
        self.model.volume = Var(self.t, within=Reals, bounds=(self.storage['VMin'], self.storage['VMax']))
        self.model.switch = Var(self.t, within=Binary)

        self.model.profit = Var(self.t, within=Reals)

        self.model.plus_limit = ConstraintList()
        self.model.minus_limit = ConstraintList()
        self.model.output_power = ConstraintList()
        self.model.volume_limit = ConstraintList()
        self.model.profit_function = ConstraintList()

        for t in self.t:
            self.model.plus_limit.add(self.model.p_plus[t] <= self.model.switch[t] * self.storage['P+_Max'])
            self.model.plus_limit.add(self.model.p_plus[t] >= self.model.switch[t] * self.storage['P+_Min'])
            self.model.minus_limit.add(self.model.p_minus[t] <= (1-self.model.switch[t]) * self.storage['P-_Max'])
            self.model.minus_limit.add(self.model.p_minus[t] >= (1- self.model.switch[t]) * self.storage['P-_Min'])

            self.model.output_power.add(self.model.p_out[t] == -self.model.p_plus[t] + self.model.p_minus[t])

            if t == 0:
                self.model.volume_0 = Constraint(self.model.volume[0] == self.storage['V0']
                                                 + self.storage['eta+'] * self.model.p_plus[0]
                                                 - self.model.p_minus[0] / self.storage['eta-'])
            else:
                self.model.volume_limit.add(self.model.volume[t] == self.model[t-1]
                                                 + self.storage['eta+'] * self.model.p_plus[0]
                                                 - self.model.p_minus[0] / self.storage['eta-'])

            self.model.profit_function.add(self.model.profit[t] == self.model.p_out[t] * self.prices['power'][t])


        # if no day ahead power known run standard optimization
        if self.committed_power is None:
            self.model.obj = Objective(expr=quicksum(self.model.profit[i] for i in self.t), sense=maximize)
        else:
            self.model.power_difference = Var(self.t, bounds=(0, None), within=Reals)
            self.model.delta_cost = Var(self.t, bounds=(0, None), within=Reals)
            self.model.minus = Var(self.t, bounds=(0, None), within=Reals)
            self.model.plus = Var(self.t, bounds=(0, None), within=Reals)

            self.model.difference = ConstraintList()
            self.model.day_ahead_difference = ConstraintList()
            self.model.difference_cost = ConstraintList()


            for t in self.t:
                self.model.difference.add(self.model.minus[t] + self.model.plus[t]
                                          == self.model.power_difference[t])

                self.model.day_ahead_difference.add(self.committed_power[t] - self.model.p_out[t]
                                                    == -self.model.minus[t] + self.model.plus[t])
                self.model.difference_cost.add(self.model.delta_cost[t]
                                               == self.model.power_difference[t] * np.abs(self.prices['power'][t] * 2))
            # set new objective
            self.model.obj = Objective(expr=quicksum(self.model.profit[i] - self.model.delta_cost[i]
                                                     for i in self.t), sense=maximize)

    def optimize(self, date=None, weather=None, prices=None, steps=None):

        if self.committed_power is None:
            self.build_model()
            self.opt.solve(self.model)

            for t in self.t:
                self.generation['total'][t] = self.model.p_out[t].value
                self.generation['water'][t] = self.model.p_out[t].value
                self.volume = self.model.volume[t].value

        return self.power
