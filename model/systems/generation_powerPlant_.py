# third party modules
import os
import numpy as np
import pandas as pd
from pyomo.environ import Constraint, Var, Objective, SolverFactory, ConcreteModel, \
    Reals, Binary, maximize, value, quicksum

# model modules
from systems.basic_system import EnergySystem
os.chdir(os.path.dirname(os.path.dirname(__file__)))


class PowerPlant(EnergySystem):

    def __init__(self, T, steps, unitID, fuel, maxPower, minPower, eta, P0, chi, stopTime, runTime, gradP, gradM,
                 on, off, startCost, *args, **kwargs):
        super().__init__(T)

        self.name = unitID
        self.power_plant = dict(fuel=fuel, maxPower=maxPower, minPower=minPower, eta=eta, P0=P0, chi=chi,
                                stopTime=stopTime, runTime=runTime, gradP=gradP, gradM=gradM, on=on, off=off)
        self.start_cost = startCost

        self.model = ConcreteModel()
        self.opt = SolverFactory('glpk')

        self.power = np.zeros((self.T,), float)
        self. emission = np.zeros((self.T,), float)
        self.fuel = np.zeros((self.T,), float)
        self.start = np.zeros((self.T,), float)

        self.steps = steps

        self.optimization_results = {step: dict(power=np.zeros(self.T, float),
                                                emission=np.zeros(self.T, float),
                                                fuel=np.zeros(self.T, float),
                                                start=np.zeros(self.T, float),
                                                obj=0) for step in steps}

        self.prevented_start = {step: dict(prevent_start=False,
                                           hours=np.zeros(self.T, float),
                                           delta=0) for step in steps}

        self.committed_power = None

    def build_model(self):

        self.model.clear()

        delta = self.power_plant['maxPower'] - self.power_plant['minPower']

        self.model.p_out = Var(self.t, bounds=(None, self.power_plant['maxPower']), within=Reals)
        self.model.gradient_0P = Constraint(expr=self.model.p_out[0] <= self.power_plant['P0']
                                                 + self.power_plant['gradP'])
        self.model.gradient_0M = Constraint(expr=self.model.p_out[0] >= self.power_plant['P0']
                                                 - self.power_plant['gradM'])

        self.model.p_opt = Var(self.t, bounds=(0, delta), within=Reals)

        # states (on, ramp up, ramp down)
        self.model.z = Var(self.t, within=Binary)
        self.model.v = Var(self.t, within=Binary)
        self.model.w = Var(self.t, within=Binary)

        def output_power(m, t):
            return m.p_out[t] == m.p_opt[t] + m.z[t] * self.power_plant['minPower']
        self.model.output_power = Constraint(self.t, rule=output_power)

        def opt_power_limit_zero(m, t):
            return 0 <= m.p_opt[t]
        self.model.opt_power_limit_zero = Constraint(self.t, rule=opt_power_limit_zero)

        def opt_power_limit_on(m, t):
            return m.p_opt[t] <= delta * m.z[t]
        self.model.opt_power_limit_on = Constraint(self.t, rule=opt_power_limit_on)

        def opt_power_limit(m, t):
            return m.p_out[t] <= self.power_plant['minPower'] * (m.z[t] + m.v[t+1] + m.p_opt[t])
        self.model.opt_power_limit = Constraint(self.t[:-1], rule=opt_power_limit)

        def ramping_up(m, t):
            return m.p_opt[t] - m.p_opt[t-1] <= self.power_plant['gradP'] * m.z[t-1]
        self.model.ramping_up = Constraint(self.t[1:], rule=ramping_up)

        def ramping_down(m, t):
            return m.p_opt[t-1] - m.p_opt[t] <= self.power_plant['gradM'] * m.z[t-1]
        self.model.ramping_down = Constraint(self.t[1:], rule=ramping_down)

        def stop_time(m, t):
            return 1 - m.z[t] >= quicksum(m.w[k] for k in range(t - self.power_plant['stopTime'], t))
        self.model.min_stop_time = Constraint(self.t[self.power_plant['stopTime']:], rule=stop_time)

        def run_time(m, t):
            return m.z[t] >= quicksum(m.v[k] for k in range(t - self.power_plant['runTime'], t))
        self.model.min_run_time = Constraint(self.t[self.power_plant['runTime']:], rule=run_time)

        def states(m, t):
            return m.z[t-1] - m.z[t] + m.v[t] - m.w[t] == 0
        self.model.states = Constraint(self.t[1:], rule=states)

        def init_state_off(m, t):
            return m.z[t] == 0

        def init_state_on(m, t):
            return m.z[t] == 1

        if self.power_plant['on'] > 0:
            self.model.on_state = Constraint(range(0, self.power_plant['runTime'] - self.power_plant['on']),
                                             rule=init_state_on)
        else:
            self.model.off_state = Constraint(range(0, self.power_plant['stopTime'] - self.power_plant['off']),
                                              rule=init_state_off)

        self.model.fuel = Var(self.t, bounds=(0, None), within=Reals)

        def fuel_cost(m, t):
            return m.fuel[t] == m.p_out[t] / self.power_plant['eta'] * self.prices[str(self.power_plant['fuel']).replace('_combined', '')][t]

        self.model.fuel_cost = Constraint(self.t, rule=fuel_cost)

        self.model.emissions = Var(self.t, bounds=(0, None), within=Reals)

        def emission_cost(m, t):
            return m.emissions[t] == m.p_out[t] * self.power_plant['chi'] * self.prices['co'][t] / self.power_plant['eta']

        self.model.emission_cost = Constraint(self.t, rule=emission_cost)

        self.model.start_ups = Var(self.t, bounds=(0, None), within=Reals)

        def start_cost(m, t):
            return m.start_ups[t] == m.v[t] * self.start_cost

        self.model.start_cost = Constraint(self.t, rule=start_cost)

        self.model.profit = Var(self.t, within=Reals)

        def profit_func(m, t):
            return m.profit[t] == m.p_out[t] * self.prices['power'][t]
        self.model.profit_func = Constraint(self.t, rule=profit_func)

        if self.committed_power is None:
            self.model.obj = Objective(expr=quicksum(self.model.profit[i] - self.model.fuel[i] - self.model.emissions[i]
                                                     - self.model.start_ups[i] for i in self.t), sense=maximize)
        else:
            self.model.delta_power = Var(self.t, bounds=(0, None), within=Reals)
            self.model.minus = Var(self.t, bounds=(0, None), within=Reals)
            self.model.plus = Var(self.t, bounds=(0, None), within=Reals)

            def delta_power(m, t):
                return m.minus[t] + m.plus[t] == m.delta_power[t]
            self.model.delta_power_constraint = Constraint(self.t, rule=delta_power)

            def minus_plus(m, t):
                return self.committed_power[t] - m.p_out[t] == -m.minus[t] + m.plus[t]
            self.model.minus_plus = Constraint(self.t, rule=minus_plus)

            self.model.delta_cost = Var(self.t, bounds=(0, None), within=Reals)

            def delta_cost(m, t):
                return m.delta_cost[t] == m.delta_power[t] * np.abs(self.prices['power'][t] * 2)
            self.model.delta_cost_constraint = Constraint(self.t, rule=delta_cost)

            self.model.obj = Objective(expr=quicksum(self.model.profit[i] - self.model.fuel[i] - self.model.emissions[i]
                                                     - self.model.start_ups[i] - self.model.delta_cost[i]
                                                     for i in self.t), sense=maximize)

    def optimize(self):

        if self.committed_power is None:

            base_price = self.prices.loc[:, 'power']
            prices_24h = self.prices.iloc[:24, :]
            prices_48h = self.prices.iloc[:48, :]

            for step in self.steps:

                self.prices = prices_24h
                self.prices.loc[:, 'power'] = base_price.iloc[:24] + step
                self.build_model()

                self.opt.solve(self.model)

                for t in self.t:
                    self.optimization_results[step]['power'][t] = self.model.p_out[t].value
                    self.optimization_results[step]['emission'][t] = self.model.emissions[t].value
                    self.optimization_results[step]['fuel'][t] = self.model.fuel[t].value
                    self.optimization_results[step]['start'][t] = self.model.start_ups[t].value
                    self.optimization_results[step]['obj'] = value(self.model.obj)

                p_out = np.asarray([self.model.p_out[t].value for t in self.t])
                objective_value = value(self.model.obj)

                if p_out[-1] == 0:
                    hours = np.argwhere(p_out == 0)
                    self.t = np.arange(48)
                    self.prices = prices_48h
                    self.prices['power'] = base_price + step
                    self.build_model()
                    self.opt.solve(self.model)
                    power_check = np.asarray([self.model.p_out[t].value for t in self.t])
                    prevent_start = all(power_check[hours] > 0)
                    delta = value(self.model.obj) - objective_value
                    percentage = delta / objective_value if objective_value else 0
                    if prevent_start and percentage > 0.05:
                        self.prevented_start.update({step: dict(prevented=True, hours=hours, delta=delta)})
                    self.t = np.arange(self.T)

                if step == 0:
                    self.cash_flow = dict(fuel=self.optimization_results[step]['fuel'],
                                          emission=self.optimization_results[step]['emission'],
                                          start_ups=self.optimization_results[step]['start'])
                    self.generation[str(self.power_plant['fuel']).replace('_combined', '')] = self.optimization_results[step]['power']
                    self.power = self.optimization_results[step]['power']

        else:
            results = self.opt.solve(self.model)
            print(results)

            for t in self.t:
                self.cash_flow['fuel'][t] = float(self.model.fuel[t].value)
                self.cash_flow['emission'][t] = float(self.model.emissions[t].value)
                self.cash_flow['start_ups'][t] = float(self.model.start_ups[t].value)
                self.power[t] = float(self.model.p_out[t].value)
                self.generation[str(self.power_plant['fuel']).replace('_combined', '')][t] = self.power[t]

            self.committed_power = None

        return self.power

if __name__ == "__main__":
    plant = {'unitID':'x',
             'fuel':'lignite',
             'maxPower': 300,
             'minPower': 100,
             'eta': 0.4,
             'P0': 120,
             'chi': 0.407,
             'stopTime': 5,
             'runTime': 10,
             'gradP': 300,
             'gradM': 300,
             'on': 1,
             'off': 0,
             'startCost': 10*10**3}

    pw = PowerPlant(T=24, steps=[-100, -10, 0, 10, 1000], **plant)

    power_price = np.ones(48) * 27 * np.random.uniform(0.95, 1.05, 48)
    co = np.ones(48) * 23.8 * np.random.uniform(0.95, 1.05, 48)     # -- Emission Price     [€/t]
    gas = np.ones(48) * 24.8 * np.random.uniform(0.95, 1.05, 48)    # -- Gas Price          [€/MWh]
    lignite =np.ones(48) * 1.5 * np.random.uniform(0.95, 1.05)                   # -- Lignite Price      [€/MWh]
    coal = np.ones(48) * 9.9 * np.random.uniform(0.95, 1.05)                      # -- Hard Coal Price    [€/MWh]
    nuc = np.ones(48) * 1.0 * np.random.uniform(0.95, 1.05)                       # -- nuclear Price      [€/MWh]

    prices = dict(power=power_price, gas=gas, co=co, lignite=lignite, coal=coal, nuc=nuc)
    prices = pd.DataFrame(data=prices, index=pd.date_range(start='2018-01-01', freq='h', periods=48))
    pw.set_parameter(date='2018-01-01', weather=None,
                     prices=prices)

    power = pw.optimize()

    power *= 0.5

    pw.committed_power = power
    pw.build_model()
    pw.optimize()