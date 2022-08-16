# third party modules
import numpy as np
import pandas as pd
from pyomo.environ import Constraint, Var, Objective, SolverFactory, ConcreteModel, \
    Reals, Binary, maximize, quicksum, ConstraintList, value

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

        self.model.p_out = Var(self.t, within=Reals, bounds=(-self.storage_system['P-_Max'],
                                                             self.storage_system['P+_Max']))
        self.model.volume = Var(self.t, within=Reals, bounds=(0, self.storage_system['VMax']))

        self.model.vol_con = ConstraintList()

        for t in self.t:
            eff = self.storage_system['eta+'] * self.storage_system['eta-']
            if t == 0:
                self.model.vol_con.add(expr=self.model.volume[t] == self.storage_system['V0'] + self.model.p_out[t] * eff)
            else:
                self.model.vol_con.add(expr=self.model.volume[t] == self.model.volume[t - 1] + self.model.p_out[t] * eff)
        self.model.vol_con.add(expr=self.model.volume[self.T-1] == self.storage_system['VMax'] / 2)

        # if no day ahead power known run standard optimization
        if committed_power is None:
            profit = [-self.model.p_out[t] * self.prices['power'][t] for t in self.t]

        # if day ahead power is known minimize the difference
        else:
            self.model.power_difference = Var(self.t, within=Reals)
            self.model.minus = Var(self.t, within=Reals, bounds=(0, None))
            self.model.plus = Var(self.t, within=Reals, bounds=(0, None))

            difference = [committed_power[t] - self.model.p_out[t] for t in self.t]

            self.model.difference = ConstraintList()
            for t in self.t:
                self.model.difference.add(self.model.plus[t]-self.model.minus[t] == difference[t])
            abs_difference = [self.model.plus[t]+self.model.minus[t] for t in self.t]
            costs = [abs_difference[t] * np.abs(self.prices['power'][t] * 2) for t in self.t]

            profit = [-self.model.p_out[t] * self.prices['power'][t] - costs[t] for t in self.t]

        self.model.obj = Objective(expr=quicksum(profit[t] for t in self.t), sense=maximize)

    def optimize_post_market(self, committed_power: np.array, power_prices: np.array = None) -> np.array:
        if power_prices is not None:
            self.prices['power'].values[:len(power_prices)] = power_prices

        self.build_model(-committed_power)
        r = self.opt.solve(self.model)
        power = np.asarray([self.model.p_out[t].value for t in self.t])

        self.power = power
        self.volume = np.asarray([self.model.volume[t].value for t in self.t])
        self.generation['total'][self.power < 0] = - self.power[self.power < 0]
        self.demand['power'][self.power > 0] = self.power[self.power > 0]
        self.generation['storage'] = self.power

        self.generation_system['VO'] = self.volume[-1]

        return self.power

    def optimize(self, date: pd.Timestamp = None, weather: pd.DataFrame = None, prices: pd.DataFrame = None,
                 steps: tuple = None):

        self._reset_data()
        self._set_parameter(date=date, weather=weather, prices=prices)

        base_price = self.prices.copy()

        for key, func in PRICE_FUNCS.items():
            self.prices['power'] = func(base_price['power'].values)
            self.build_model()
            r = self.opt.solve(self.model)
            power = np.asarray([self.model.p_out[t].value for t in self.t])
            self.opt_results[key] = power
            if key == 'normal':
                self.power = power
                self.volume = np.asarray([self.model.volume[t].value for t in self.t])
                self.generation['total'][self.power < 0] = - self.power[self.power < 0]
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
                                  price=prc[t], volume=-power[t])
                          for t in self.t}
            df = pd.DataFrame.from_dict(order_book, orient='index')
            total_orders += [df]
        df = pd.concat(total_orders, axis=0)
        df = df.set_index(['block_id', 'hour', 'name'])
        return df


if __name__ == "__main__":
    from utils import get_test_prices
    from matplotlib import pyplot as plt

    storage = {"eta_plus": 0.8, "eta_minus": 0.87, "fuel": "water", "PPlus_max": 10,
               "PMinus_max": 10, "V0": 50, "VMin": 0, "VMax": 100}

    sys = Storage(T=24, unitID='x', **storage)
    storage_prices = get_test_prices()
    storage_prices['power'][:8] = 100
    pw1 = sys.optimize(prices=storage_prices)
    plt.plot(sys.generation['storage'])
    orders = sys.get_exclusive_orders()
    market_result = orders.loc[orders.index.get_level_values('block_id') == 0, 'volume'].values
    pw2 = sys.optimize_post_market(market_result * 0.8)
    plt.plot(sys.generation['storage'])

    # plt.plot(pw1)
    # plt.plot(pw2)