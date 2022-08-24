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


def shaping(prc, type_: str = 'peak'):
    if type_ == 'peak':
        prc[8:20] *= 1.5
    elif type_ == 'pv':
        prc[11:13] *= 0.6
    elif type_ == 'demand':
        prc[6:9] *= 1.5
        prc[17:20] *= 1.5
    return prc


PRICE_FUNCS = {'left': lambda prc: np.roll(prc, -1),
               'right': lambda prc: np.roll(prc, 1),
               'normal': lambda prc: prc,
               'first': lambda prc: shift(prc, type_='first'),
               'last': lambda prc: shift(prc, type_='last'),
                # 'peak_off_peak': lambda prc: shaping(prc, type_='peak'),
               'pv_sink:': lambda prc: shaping(prc, type_='pv'),
               'demand': lambda prc: shaping(prc, type_='demand')}


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

        self.model.p_plus = Var(self.t, within=Reals, bounds=(0, self.storage_system['P+_Max']))
        self.model.p_minus = Var(self.t, within=Reals, bounds=(0, self.storage_system['P-_Max']))
        self.model.volume = Var(self.t, within=Reals, bounds=(0, self.storage_system['VMax']))

        power = [-self.model.p_minus[t]/self.storage_system['eta-'] + self.model.p_plus[t] * self.storage_system['eta+']
                 for t in self.t]

        self.model.vol_con = ConstraintList()

        for t in self.t:
            if t == 0:
                self.model.vol_con.add(expr=self.model.volume[t] == self.storage_system['V0'] + power[t])
            else:
                self.model.vol_con.add(expr=self.model.volume[t] == self.model.volume[t - 1] + power[t])

        self.model.vol_con.add(expr=self.model.volume[self.T-1] == self.storage_system['VMax'] / 2)

        # if no day ahead power known run standard optimization
        if committed_power is None:
            profit = [-power[t] * self.prices['power'][t] for t in self.t]

        # if day ahead power is known minimize the difference
        else:
            self.model.power_difference = Var(self.t, within=Reals)
            self.model.minus = Var(self.t, within=Reals, bounds=(0, None))
            self.model.plus = Var(self.t, within=Reals, bounds=(0, None))

            difference = [committed_power[t] - power[t] for t in self.t]

            self.model.difference = ConstraintList()
            for t in self.t:
                self.model.difference.add(self.model.plus[t]-self.model.minus[t] == difference[t])
            abs_difference = [self.model.plus[t]+self.model.minus[t] for t in self.t]
            costs = [abs_difference[t] * np.abs(self.prices['power'][t] * 2) for t in self.t]

            profit = [-power[t] * self.prices['power'][t] - costs[t] for t in self.t]

        self.model.obj = Objective(expr=quicksum(profit[t] for t in self.t), sense=maximize)

    def optimize_post_market(self, committed_power: np.array, power_prices: np.array = None) -> np.array:
        if power_prices is not None:
            self.prices['power'].values[:len(power_prices)] = power_prices

        self.build_model(-committed_power)
        r = self.opt.solve(self.model)
        power = np.asarray([-self.model.p_minus[t].value/self.storage_system['eta-']
                            + self.model.p_plus[t].value * self.storage_system['eta+'] for t in self.t])

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
            power = np.asarray([-self.model.p_minus[t].value * self.storage_system['eta-']
                                + self.model.p_plus[t].value for t in self.t])
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
        total_orders = {}
        block_id = 0
        power_prices = self.prices['power'].copy()
        for key, power in self.opt_results.items():
            prc = np.zeros(self.T)
            bid_hours = np.argwhere(power < 0).flatten()
            ask_hours = np.argwhere(power > 0).flatten()
            max_charging_price = power_prices.values[bid_hours].max()
            min_discharging_price = max_charging_price / (self.storage_system['eta+'] * self.storage_system['eta-'])
            prc[ask_hours] = (power_prices[ask_hours] + min_discharging_price) / 2
            prc[bid_hours] = power_prices.values[bid_hours]
            add = True
            for orders in total_orders.values():
                if any(prc != orders['price']) or any(power != orders['volume']):
                    add = False
            if add:
                total_orders[block_id] = dict(price=prc, volume=power)
                block_id += 1
        dfs = []
        for key, values in total_orders.items():
            df = pd.DataFrame(data=values)
            df['name'] = self.name
            df['block_id'] = key
            df['hour'] = self.t
            df = df.set_index(['block_id', 'hour', 'name'])
            dfs.append(df)

        df = pd.concat(dfs)

        return df


if __name__ == "__main__":
    from utils import get_test_prices
    from matplotlib import pyplot as plt

    storage = {"eta_plus": 0.8, "eta_minus": 0.87, "fuel": "water", "PPlus_max": 10,
               "PMinus_max": 10, "V0": 50, "VMin": 0, "VMax": 100}

    sys = Storage(T=24, unitID='x', **storage)
    storage_prices = get_test_prices()
    # storage_prices['power'][:8] = 100
    pw1 = sys.optimize(prices=storage_prices)
    # plt.plot(sys.generation['storage'])
    orders = sys.get_exclusive_orders()
    market_result = orders.loc[orders.index.get_level_values('block_id') == 0, 'volume'].values
    # pw2 = sys.optimize_post_market(market_result * 0.8)
    # plt.plot(sys.generation['storage'])

    for k in range(20,39):
        plt.plot(orders.loc[orders.index.get_level_values('block_id') == k, 'volume'].values)

    #plt.plot(pw1)
    #plt.plot(pw2)