# third party modules
import numpy as np
import pandas as pd
from pyomo.environ import Constraint, Var, Objective, SolverFactory, ConcreteModel, \
    Reals, Binary, maximize, quicksum, ConstraintList, NonNegativeReals, minimize, value

# model modules
from systems.basic_system import EnergySystem


def shift(prc, type_: str = 'first'):
    new_prices, num_prices = [], len(prc)

    for p, index in zip(prc, range(num_prices)):
        if type_ == 'first':
            new_prices += [p * (0.9 + index * 0.2 / num_prices)]
        else:
            new_prices += [p * (1.1 - index * 0.2 / num_prices)]

    return np.asarray(new_prices)


PRICE_FUNCS = {'left': lambda prc: np.roll(prc, -1),
               'right': lambda prc: np.roll(prc, 1),
               'normal': lambda prc: prc,
               'first': lambda prc: shift(prc, type_='first'),
               'last': lambda prc: shift(prc, type_='last')}


class Storage(EnergySystem):

    def __init__(self, T: int, unitID: str, eta_plus: float, eta_minus: float,
                 V0: float, VMin: float, VMax: float, PPlus_max: float, PMinus_max: float, *args, **kwargs):
        super().__init__(T)

        self.name = unitID
        self.storage_system = {"eta+": eta_plus, "eta-": eta_minus, "fuel": "water", "P+_Max": PPlus_max,
                               "P-_Max": PMinus_max, "V0": V0, "VMin": VMin, "VMax": VMax}

        self.opt_results = {key: np.zeros(self.T) for key in PRICE_FUNCS.keys()}

        self.model = ConcreteModel()
        self.opt = SolverFactory('glpk')

    def build_model(self, committed_power: np.array = None):

        self.model.clear()

        self.model.p_plus = Var(self.t, within=NonNegativeReals)
        self.model.p_minus = Var(self.t, within=NonNegativeReals)
        self.model.volume = Var(self.t, within=NonNegativeReals)
        self.model.switch = Var(self.t, within=Binary)

        self.model.plus_con = ConstraintList()
        self.model.minus_con = ConstraintList()
        self.model.vol_con = ConstraintList()
        self.model.profit_function = ConstraintList()

        for t in self.t:
            self.model.plus_con.add(self.model.p_plus[t] <= self.model.switch[t] * self.storage_system['P+_Max'])
            self.model.minus_con.add(
                self.model.p_minus[t] <= (1 - self.model.switch[t]) * self.storage_system['P-_Max'])
            self.model.vol_con.add(self.model.volume[t] <= self.storage_system['VMax'])

        for t in self.t:
            charged = self.storage_system['eta+'] * self.model.p_plus[t]
            discharge = self.model.p_minus[t] / self.storage_system['eta-']
            if t == 0:
                self.model.vol_con.add(expr=self.model.volume[t] == self.storage_system['V0'] + charged - discharge)
            elif t == 23:
                self.model.vol_con.add(expr=self.model.volume[t] + charged - discharge >= self.storage_system['V0']/3)
            else:
                self.model.vol_con.add(expr=self.model.volume[t] == self.model.volume[t - 1] + charged - discharge)

        # if no day ahead power known run standard optimization
        if committed_power is None:
            p_out = [-self.model.p_plus[t] + self.model.p_minus[t] for t in self.t]
            profit = [p_out[t] * self.prices['power'][t] for t in self.t]

        # if day ahead power is known minimize the difference
        else:
            p_out = [-self.model.p_plus[t] + self.model.p_minus[t] for t in self.t]
            self.model.power_difference = Var(self.t, within=NonNegativeReals)
            self.model.minus = Var(self.t, within=NonNegativeReals)
            self.model.plus = Var(self.t, within=NonNegativeReals)

            difference = [self.model.minus[t] + self.model.plus[t] for t in self.t]
            self.model.difference = ConstraintList()
            for t in self.t:
                self.model.difference.add(committed_power[t] - p_out[t]
                                          == -self.model.minus[t] + self.model.plus[t])
            difference_cost = [difference[t] * np.abs(self.prices['power'][t] * 2) for t in self.t]
            profit = [p_out[t] * self.prices['power'][t] - difference_cost[t] for t in self.t]
            # set new objective

        self.model.obj = Objective(expr=quicksum(profit[t] for t in self.t), sense=maximize)

    def optimize_post_market(self, committed_power: np.array, power_prices: np.array = None):
        if power_prices is not None:
            self.prices['power'].values[:len(power_prices)] = power_prices

        self.build_model(committed_power)
        self.opt.solve(self.model)

        power = np.asarray([self.model.p_plus[t].value - self.model.p_minus[t].value for t in self.t])

        self.power = power
        self.volume = np.asarray([self.model.volume[t] for t in self.t])
        self.generation['total'][self.power < 0] = self.power[self.power < 0]
        self.demand['power'][self.power > 0] = self.power[self.power > 0]
        self.generation['storage'] = self.power

        self.generation_system['VO'] = self.volume[-1]

    def optimize(self, date: pd.Timestamp = None, weather: pd.DataFrame = None, prices: pd.DataFrame = None,
                 steps: tuple = None):

        self._reset_data()
        self._set_parameter(date=date, weather=weather, prices=prices)

        base_price = self.prices.copy()

        for key, func in PRICE_FUNCS.items():
            self.prices['power'] = func(base_price['power'].values)
            self.build_model()
            self.opt.solve(self.model)
            power = np.asarray([self.model.p_plus[t].value - self.model.p_minus[t].value for t in self.t])
            self.opt_results[key] = power
            if key == 'normal':
                self.power = power
                self.volume = np.asarray([self.model.volume[t] for t in self.t])
                self.generation['total'][self.power < 0] = self.power[self.power < 0]
                self.demand['power'][self.power > 0] = self.power[self.power > 0]
                self.generation['storage'] = self.power

        self.prices['power'] = base_price['power'].values

        return self.power

    def get_exclusive_orders(self) -> pd.DataFrame:
        total_orders = []
        for result, num in zip(self.opt_results.items(), range(len(self.opt_results))):
            key, power = result
            prc = PRICE_FUNCS[key](self.prices['power'].values)
            order_book = {t: dict(hour=t, block_id=num, name=self.name,
                                  price=prc[t], volume=power[t])
                          for t in self.t}
            df = pd.DataFrame.from_dict(order_book, orient='index')
            total_orders += [df]
        df = pd.concat(total_orders, axis=0)
        df = df.set_index(['block_id', 'hour', 'name'])
        return df


if __name__ == "__main__":
    storage = {"eta_plus": 0.8, "eta_minus": 0.87, "fuel": "water", "PPlus_max": 10,
               "PMinus_max": 10, "V0": 0, "VMin": 0, "VMax": 100}

    sys = Storage(T=24, unitID='x', **storage)

    power_price = np.ones(48)  # * np.random.uniform(0.95, 1.05, 48) # €/kWh
    power_price[12:] = 3

    co = np.ones(48) * 23.8  # * np.random.uniform(0.95, 1.05, 48)     # -- Emission Price     [€/t]
    gas = np.ones(48) * 0.03  # * np.random.uniform(0.95, 1.05, 48)    # -- Gas Price          [€/kWh]
    lignite = np.ones(48) * 0.015  # * np.random.uniform(0.95, 1.05)   # -- Lignite Price      [€/kWh]
    coal = np.ones(48) * 0.02  # * np.random.uniform(0.95, 1.05)       # -- Hard Coal Price    [€/kWh]
    nuc = np.ones(48) * 0.01  # * np.random.uniform(0.95, 1.05)        # -- nuclear Price      [€/kWh]

    prices = dict(power=power_price, gas=gas, co=co, lignite=lignite, coal=coal, nuc=nuc)
    prices = pd.DataFrame(data=prices, index=pd.date_range(start='2018-01-01', freq='h', periods=48))

    pw = sys.optimize(prices=prices)

    orders = sys.get_exclusive_orders()

    # # if no day ahead power known run standard optimization
    # # if self.committed_power is None:
    #     self.model.obj = Objective(expr=quicksum(self.model.profit[i] for i in self.t), sense=maximize)
    # else:
    #     self.model.power_difference = Var(self.t, bounds=(0, None), within=Reals)
    #     self.model.delta_cost = Var(self.t, bounds=(0, None), within=Reals)
    #     self.model.minus = Var(self.t, bounds=(0, None), within=Reals)
    #     self.model.plus = Var(self.t, bounds=(0, None), within=Reals)
    #
    #     self.model.difference = ConstraintList()
    #     self.model.day_ahead_difference = ConstraintList()
    #     self.model.difference_cost = ConstraintList()
    #
    #
    #     for t in self.t:
    #         self.model.difference.add(self.model.minus[t] + self.model.plus[t]
    #                                   == self.model.power_difference[t])
    #
    #         self.model.day_ahead_difference.add(self.committed_power[t] - self.model.p_out[t]
    #                                             == -self.model.minus[t] + self.model.plus[t])
    #         self.model.difference_cost.add(self.model.delta_cost[t]
    #                                        # == self.model.power_difference[t] * np.abs(self.prices['power'][t] * 2))
