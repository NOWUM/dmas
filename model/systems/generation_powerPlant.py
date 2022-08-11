# third party modules
import numpy as np
import pandas as pd
from pyomo.environ import Constraint, Var, Objective, SolverFactory, ConcreteModel, \
    NonNegativeReals, Reals, Binary, maximize, value, quicksum, ConstraintList
from matplotlib import pyplot as plt

# model modules
from systems.basic_system import EnergySystem

import logging

log = logging.getLogger('powerplant_gen')


class PowerPlant(EnergySystem):

    def __init__(self, T, steps, unitID, fuel, maxPower, minPower, eta, P0, chi, stopTime, runTime, gradP, gradM,
                 on, off, startCost, *args, **kwargs):
        super().__init__(T)

        self.base_price = None

        self.name = unitID

        # PWP solves in kW as all others too
        self.power_plant = dict(fuel=fuel, maxPower=maxPower, minPower=minPower, eta=eta, P0=P0, chi=chi,
                                stopTime=stopTime, runTime=runTime, gradP=gradP, gradM=gradM, on=on, off=off)
        self.start_cost = startCost

        self.model = ConcreteModel()
        self.opt = SolverFactory('glpk')

        self.power = np.zeros((self.T,), float)

        self.steps = steps

        self.optimization_results = {step: dict(power=np.zeros(self.T, float),
                                                emission=np.zeros(self.T, float),
                                                fuel=np.zeros(self.T, float),
                                                start=np.zeros(self.T, float),
                                                profit=np.zeros(self.T, float),
                                                obj=0) for step in steps}

        self.prevented_start = dict(prevent=False, hours=np.zeros(self.T, float), delta=0)

    def set_parameter(self, date, weather=None, prices=None):
        super().set_parameter(date, weather, prices)
        self.base_price = prices.copy()

    def build_model(self, committed_power=None):

        self.model.clear()

        delta = self.power_plant['maxPower'] - self.power_plant['minPower']

        self.model.p_out = Var(self.t, bounds=(0, self.power_plant['maxPower']), within=Reals)
        self.model.p_model = Var(self.t, bounds=(0, delta), within=Reals)

        # states (on, ramp up, ramp down)
        self.model.z = Var(self.t, within=Binary)
        self.model.v = Var(self.t, within=Binary)
        self.model.w = Var(self.t, within=Binary)

        # cash flow variables
        # XXX not opt variable - can be pyomo expression
        self.model.fuel = Var(self.t, within=NonNegativeReals, initialize=0)
        self.model.emissions = Var(self.t, within=NonNegativeReals, initialize=0)
        self.model.start_ups = Var(self.t, within=NonNegativeReals, initialize=0)
        self.model.profit = Var(self.t, within=Reals, initialize=0)

        # define constraint for output power
        self.model.real_power = ConstraintList()
        self.model.real_max = ConstraintList()
        # define constraint for model power
        self.model.model_min = ConstraintList()
        self.model.model_max = ConstraintList()
        # define constraint ramping
        self.model.ramping_up = ConstraintList()
        self.model.ramping_down = ConstraintList()
        # define constraint for run- and stop-time
        self.model.stop_time = ConstraintList()
        self.model.run_time = ConstraintList()
        self.model.states = ConstraintList()
        self.model.initial_on = ConstraintList()
        self.model.initial_off = ConstraintList()
        # define constraint for cash-flow aspects
        self.model.fuel_cost = ConstraintList()
        self.model.emission_cost = ConstraintList()
        self.model.start_cost = ConstraintList()
        self.model.profit_function = ConstraintList()

        try:
            fuel_prices = self.prices[str(self.power_plant['fuel']).replace('_combined', '')]
        except KeyError as e:
            log.error(f'prices were: {self.prices}')
            raise Exception(f"No Fuel prices given for fuel {self.power_plant['fuel']}")

        for t in self.t:
            # output power of the plant
            self.model.real_power.add(self.model.p_out[t] == self.model.p_model[t] + self.model.z[t]
                                      * self.power_plant['minPower'])
            if t < 23:  # only the next day
                self.model.real_max.add(self.model.p_out[t] <= self.power_plant['minPower']
                                        * (self.model.z[t] + self.model.v[t + 1] + self.model.p_model[t]))
            # model power for optimization
            self.model.model_min.add(0 <= self.model.p_model[t])
            self.model.model_max.add(self.model.z[t] * delta >= self.model.p_model[t])
            # ramping (gradients)
            if t == 0:
                self.model.ramping_up_0 = Constraint(expr=self.model.p_out[0] <= self.power_plant['P0']
                                                          + self.power_plant['gradP'])
                self.model.ramping_down_0 = Constraint(expr=self.model.p_out[0] >= self.power_plant['P0']
                                                            - self.power_plant['gradM'])
            else:
                self.model.ramping_up.add(self.model.p_model[t] - self.model.p_model[t - 1]
                                          <= self.power_plant['gradP'] * self.model.z[t - 1])
                self.model.ramping_down.add(self.model.p_model[t - 1] - self.model.p_model[t]
                                            <= self.power_plant['gradM'] * self.model.z[t - 1])
            # minimal run and stop time
            if t > self.power_plant['stopTime']:
                self.model.stop_time.add(1 - self.model.z[t]
                                         >= quicksum(
                    self.model.w[k] for k in range(t - self.power_plant['stopTime'], t)))
            if t > self.power_plant['runTime']:
                self.model.run_time.add(self.model.z[t]
                                        >= quicksum(self.model.v[k] for k in range(t - self.power_plant['runTime'], t)))
            if t > 0:
                self.model.states.add(self.model.z[t - 1] - self.model.z[t] + self.model.v[t] - self.model.w[t] == 0)

            if self.power_plant['on'] > 0 and t < self.power_plant['runTime'] - self.power_plant['on']:
                self.model.initial_on.add(self.model.z[t] == 1)
            elif self.power_plant['off'] > 0 and t < self.power_plant['stopTime'] - self.power_plant['off']:
                self.model.initial_off.add(self.model.z[t] == 0)

            self.model.fuel_cost.add(self.model.fuel[t]
                                     == self.model.p_out[t] / self.power_plant['eta']
                                     * fuel_prices[t])

            self.model.emission_cost.add(self.model.emissions[t]
                                         == self.model.p_out[t] / self.power_plant['eta']
                                         * self.power_plant['chi'] * self.prices['co'][t])

            self.model.start_cost.add(self.model.start_ups[t] == self.model.v[t] * self.start_cost)

            self.model.profit_function.add(self.model.profit[t] == self.model.p_out[t] * self.prices['power'][t])

        # if no day ahead power known run standard optimization
        if committed_power is None:
            self.model.obj = Objective(expr=quicksum(self.model.profit[i] - self.model.fuel[i] - self.model.emissions[i]
                                                     - self.model.start_ups[i] for i in self.t), sense=maximize)
        # if day ahead power is known minimize the difference
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

                self.model.day_ahead_difference.add(committed_power[t] - self.model.p_out[t]
                                                    == -self.model.minus[t] + self.model.plus[t])
                self.model.difference_cost.add(self.model.delta_cost[t]
                                               == self.model.power_difference[t] * np.abs(self.prices['power'][t] * 2))

            # set new objective
            self.model.obj = Objective(expr=quicksum(self.model.profit[i] - self.model.fuel[i] - self.model.emissions[i]
                                                     - self.model.start_ups[i] - self.model.delta_cost[i]
                                                     for i in self.t), sense=maximize)

    def optimize(self, date, weather=None, prices=None, steps=None):
        self.set_parameter(date, weather, prices)
        steps = steps or self.steps
        prices_24h = self.base_price.iloc[:24, :].copy()
        prices_48h = self.base_price.iloc[:48, :].copy()

        for step in steps:
            self.prices = prices_24h
            self.prices.loc[:, 'power'] = self.base_price.iloc[:24]['power'] + step
            self.build_model()

            results = self.opt.solve(self.model)

            for t in self.t:
                self.optimization_results[step]['power'][t] = self.model.p_out[t].value
                self.optimization_results[step]['emission'][t] = self.model.emissions[t].value
                self.optimization_results[step]['fuel'][t] = self.model.fuel[t].value
                self.optimization_results[step]['start'][t] = self.model.start_ups[t].value
                self.optimization_results[step]['obj'] = value(self.model.obj)
                self.optimization_results[step]['profit'][t] = value(self.model.profit[t].value)

            p_out = np.asarray([self.model.p_out[t].value for t in self.t])
            objective_value = value(self.model.obj)

            if p_out[-1] == 0 and step == 0:
                all_off = np.argwhere(p_out == 0).flatten()
                last_on = np.argwhere(p_out > 0).flatten()
                last_on = last_on[-1] if len(last_on) > 0 else 0
                prevented_off_hours = all_off[all_off > last_on]

                self.t = np.arange(48)
                self.prices = prices_48h
                self.prices.loc[:, 'power'] = self.base_price.iloc[:48]['power']
                self.build_model()
                self.opt.solve(self.model)
                power_check = np.asarray([self.model.p_out[t].value for t in self.t])
                prevent_start = all(power_check[prevented_off_hours] > 0)
                delta = value(self.model.obj) - objective_value
                percentage = delta / objective_value if objective_value else 0
                if prevent_start and percentage > 0.05:
                    self.prevented_start = dict(prevent=True, hours=prevented_off_hours,
                                                delta=delta / (len(prevented_off_hours) * self.power_plant['minPower']))
                self.t = np.arange(self.T)

            if step == 0:
                self.cash_flow = dict(fuel=self.optimization_results[step]['fuel'],
                                      emission=self.optimization_results[step]['emission'],
                                      start_ups=self.optimization_results[step]['start'],
                                      profit=self.optimization_results[step]['profit'])
                self.generation[str(self.power_plant['fuel']).replace('_combined', '')] = \
                    self.optimization_results[step]['power']
                self.power = self.optimization_results[step]['power']
        return self.power

    def optimize_post_market(self, committed_power, steps=None):
        steps = steps or self.steps
        self.build_model(committed_power)
        self.opt.solve(self.model)
        running_since = 0
        off_since = 0
        for t in self.t:
            self.cash_flow['fuel'][t] = float(self.model.fuel[t].value)
            self.cash_flow['emission'][t] = float(self.model.emissions[t].value)
            self.cash_flow['start_ups'][t] = float(self.model.start_ups[t].value)
            self.cash_flow['profit'][t] = float(self.model.profit[t].value)
            self.power[t] = float(self.model.p_out[t].value)
            self.generation[str(self.power_plant['fuel']).replace('_combined', '')][t] = self.power[t]

            # write to opt_results for all steps - to overwrite old data
            # needed for correct order_book view
            for step in steps:
                self.optimization_results[step]['power'][t] = self.model.p_out[t].value
                self.optimization_results[step]['emission'][t] = self.model.emissions[t].value
                self.optimization_results[step]['fuel'][t] = self.model.fuel[t].value
                self.optimization_results[step]['start'][t] = self.model.start_ups[t].value
                self.optimization_results[step]['obj'] = value(self.model.obj)
                self.optimization_results[step]['profit'][t] = value(self.model.profit[t].value)

            # find count of last 1s and 0s
            if self.model.z[t].value > 0:
                running_since += 1
                off_since = 0
            else:
                running_since = 0
                off_since += 1

        self.power_plant['P0'] = self.power[-1]
        self.power_plant['on'] = running_since
        self.power_plant['off'] = off_since

        return self.power.copy()

    def get_clean_spread(self, prices=None):
        prices = prices or self.prices
        return 1 / self.power_plant['eta'] * (prices[self.power_plant['fuel'].replace('_combined', '')].mean()
                                              + self.power_plant['chi'] * prices['co'].mean())

    def get_orderbook(self) -> pd.DataFrame:

        def set_order(r: dict, h: int, block_nr: int, link: dict, mother: bool = False,
                      lst_pwr: np.array = np.zeros(self.T), add: int = 0):
            book = {}
            pwr = r['power'][h]
            if mother:
                prc = (r['fuel'][h] + r['emission'][h] + r['start'][h]) / pwr if pwr else 1e6
                book[(block_nr, h, self.name)] = (prc, pwr - lst_pwr[h], -1)
            else:
                prc = (r['fuel'][h] + r['emission'][h]) / pwr if pwr else 1e6
                l = 0 if link[h + add] is None else link[h + add]
                book[(block_nr, h, self.name)] = (prc, (pwr - lst_pwr[h]), l)
            return book

        # -> initialize empty dict
        order_book = {}
        last_power = np.zeros(self.T)
        block_number = 0
        links = {i: None for i in self.t}  # -> links[hour] = last_block

        for step in self.steps:

            # -> get optimization result for key (block) and step
            result = self.optimization_results[step]
            if any(result['power'] > 0) and block_number == 0:
                # so lange wie ich muss
                hours_needed_to_run = (self.power_plant['runTime'] - self.power_plant['on'])

                if hours_needed_to_run < 1 and result['power'][0] > 0:
                    order_book.update(set_order(r=result, h=0, block_nr=block_number, mother=True, link=links))
                    links[0] = block_number
                    last_power[0] = result['power'][0]  # -> set last_power to current power
                else:
                    hours = np.argwhere(result['power'] > 0).flatten()
                    total_start_cost = result['start'][hours[0]]
                    result['power'][hours] = self.power_plant['minPower']
                    result['start'][hours] = total_start_cost / sum(result['power'][hours])
                    for hour in hours:
                        order_book.update(set_order(r=result, h=hour, block_nr=block_number, mother=True, link=links))
                        links[hour] = block_number
                        last_power[hour] = result['power'][hour]  # -> set last_power to current power
                block_number += 1  # -> increment block number

            # -> add linked hour blocks
            # -> check if current power is higher than the last known power
            if any(result['power'] - last_power > 0):
                delta = result['power'] - last_power
                # -> add on top
                for hour in self.t:
                    if delta[hour] > 0 and last_power[hour] > 0:
                        order_book.update(set_order(r=result, h=hour, block_nr=block_number,
                                                    link=links, lst_pwr=last_power))
                        last_power[hour] += delta[hour]
                        links[hour] = block_number  # -> update last known block for hour
                        block_number += 1  # -> increment block number
                # -> add left and right
                stack = np.argwhere(last_power > 0).flatten()
                if len(stack) > 0:
                    delta = result['power'] - last_power
                    for start, end, inc in zip([stack[-1], stack[0]], [self.T, 0], [1, -1]):
                        for hour in range(start, end, inc):
                            if delta[hour] > 0:
                                order_book.update(set_order(r=result, h=hour, block_nr=block_number,
                                                            link=links, lst_pwr=last_power, add=-inc))
                                last_power[hour] += delta[hour]
                                links[hour] = block_number
                                block_number += 1
        if order_book:
            df = pd.DataFrame.from_dict(order_book, orient='index')
        else:
            # if nothing in self.portfolio.energy_systems
            df = pd.DataFrame(columns=['price', 'volume', 'link', 'type'])

        df['type'] = 'generation'
        df.columns = ['price', 'volume', 'link', 'type']
        df.index = pd.MultiIndex.from_tuples(df.index, names=['block_id', 'hour', 'name'])

        if self.prevented_start['prevent']:

            hours = self.prevented_start['hours']
            # -> get cost for running on minPower
            print(self.power_plant)
            fuel_prices = self.prices[str(self.power_plant['fuel']).replace('_combined', '')].values
            print('Fuel Prices: ', fuel_prices)
            em_prices = self.prices['co'].values
            print('Emission Prices: ', em_prices)
            fuel = (self.power_plant['minPower'] / self.power_plant['eta']) * fuel_prices
            emission = ((self.power_plant['minPower'] / self.power_plant['eta']) * self.power_plant['chi']) * em_prices
            print('Power Plant: ', self.name)
            print('fuel cost: ', fuel[hours])
            print('emission cost: ', emission[hours])
            print('delta: ',  self.prevented_start['delta'])
            min_price = np.mean(fuel[hours] + emission[hours]) - self.prevented_start['delta']
            print('Pirce: ', min_price)

            # -> volume and price which is already in orderbook
            normal_volume = df.loc[:, df.index.get_level_values('hour').isin(hours), :]['volume']
            normal_price = df.loc[:, df.index.get_level_values('hour').isin(hours), :]['price']
            # -> drop volume and price in these hours and build new orders
            df = df.loc[~df.index.get_level_values('hour').isin(hours)]
            # -> get last block to link on
            last_block = max(df.index.get_level_values('block_id').values) if len(df) > 0 else -1

            # -> build new orders
            prev_order = {}
            block_number = last_block + 1
            # -> for each hour build one block with minPower
            for hour in hours:
                prev_order[(block_number, hour, self.name)] = (min_price, self.power_plant['minPower'],
                                                               last_block, 'generation')
            last_block = block_number
            block_number += 1
            for index in normal_volume.index:
                vol = normal_volume.loc[index] - self.power_plant['minPower']
                prc = normal_price.loc[index]
                _, hour, _ = index
                if vol > 0:
                    prev_order[(block_number, hour, self.name)] = (prc, vol, last_block, 'generation')
                    block_number += 1

            df_prev = pd.DataFrame.from_dict(prev_order, orient='index')
            df_prev.columns = ['price', 'volume', 'link', 'type']
            df_prev.index = pd.MultiIndex.from_tuples(df_prev.index, names=['block_id', 'hour', 'name'])
            # -> limit to market price range
            df = pd.concat([df, df_prev], axis=0)
            df.loc[df['price'] < -500/1e3, 'price'] = -500/1e3

        return df


def visualize_orderbook(order_book):
    import matplotlib.pyplot as plt
    from matplotlib.colors import ListedColormap
    tab20_cmap = plt.get_cmap("tab20c")
    ob = order_book.reset_index(level=[0, 2])
    idx = np.arange(24)

    y_past = np.zeros(24)
    for i, df_grouped in ob.groupby('block_id'):
        my_cmap_raw = np.array(tab20_cmap.colors) * i / 24
        my_cmap = ListedColormap(my_cmap_raw)

        for j, o in df_grouped.groupby('link'):
            x = idx  # o.index
            ys = np.zeros(24)
            ys[o.index] = o['volume']
            if len(list(ys)) > 0 and (ys > 0).any():
                plt.bar(x, ys, bottom=y_past, color=my_cmap.colors[(j + 1) % 20])
            y_past += ys
    plt.title('Orderbook')
    plt.xlabel('hour')
    plt.ylabel('kW')
    plt.show()


def test_half_power(plant, prices):
    pwp = PowerPlant(T=24, steps=steps, **plant)
    power = pwp.optimize('2018-01-01', None, prices)
    o_book = pwp.get_orderbook()
    # running since 1
    visualize_orderbook(o_book)

    # clean_spread = (1/plant['eta'] * (prices[plant['fuel']].mean() + plant['chi'] * prices['co'].mean()))
    print(f'{pwp.get_clean_spread()} €/kWh cost')
    print(f"{pwp.power_plant['maxPower'] * pwp.get_clean_spread()} €/h full operation")

    # assume market only gives you halve of your offers
    pwp.optimize_post_market(committed_power=power / 2)

    # running since 1
    visualize_orderbook(pwp.get_orderbook())

    assert all((power / 2 - pwp.power) < 1e-10)  # smaller than threshold
    assert pwp.power_plant['on'] == 24
    assert pwp.power_plant['off'] == 0


def test_ramp_down(plant, prices):
    pwp = PowerPlant(T=24, steps=steps, **plant)
    power = pwp.optimize('2018-01-01', None, prices)

    visualize_orderbook(pwp.get_orderbook())

    # power plant should ramp down correctly
    pwp.optimize_post_market(committed_power=power * 0)
    visualize_orderbook(pwp.get_orderbook())

    assert pwp.power_plant['on'] == 0
    assert pwp.power_plant['off'] == 19

    power_day2 = pwp.optimize('2018-01-02', None, prices)

    visualize_orderbook(pwp.get_orderbook())

    # another day off - this time a full day
    pwp.optimize_post_market(committed_power=power * 0)
    visualize_orderbook(pwp.get_orderbook())

    assert pwp.power_plant['on'] == 0
    assert pwp.power_plant['off'] == 24

    # for k,v in pwp.optimization_results.items(): print(k, v['power'])
    # for k,v in pwp.optimization_results.items(): print(k, v['obj'])
    # actual schedule corresponds to the market result


def test_minimize_diff(plant, prices):
    pwp = PowerPlant(T=24, steps=steps, **plant)
    power = pwp.optimize('2018-01-01', None, prices)

    visualize_orderbook(pwp.get_orderbook())

    # power plant has to minimize loss, when market did something weird
    p = power.copy()
    p[4:10] = 0
    power_day2 = pwp.optimize_post_market(committed_power=p)
    visualize_orderbook(pwp.get_orderbook())

    return pwp


def test_up_down(plant, prices):
    pwp = PowerPlant(T=24, steps=steps, **plant)
    power = pwp.optimize('2018-01-01', None, prices)

    visualize_orderbook(pwp.get_orderbook())

    # power plant has to minimize loss, when market did something weird
    p = power.copy()
    p[::2] = 0
    power_day2 = pwp.optimize_post_market(committed_power=p)
    visualize_orderbook(pwp.get_orderbook())
    return pwp


if __name__ == "__main__":
    plant = {'unitID': 'x',
             'fuel': 'lignite',
             'maxPower': 300,  # kW
             'minPower': 100,  # kW
             'eta': 0.4,  # Wirkungsgrad
             'P0': 120,
             'chi': 0.407 / 1e3,  # t CO2/kWh
             'stopTime': 12,  # hours
             'runTime': 6,  # hours
             'gradP': 300,  # kW/h
             'gradM': 300,  # kW/h
             'on': 1,  # running since
             'off': 0,
             'startCost': 1e3  # €/Start
             }
    steps = np.array([-100, 0, 100])

    power_price = np.ones(48)  # * np.random.uniform(0.95, 1.05, 48) # €/kWh
    co = np.ones(48) * 23.8  # * np.random.uniform(0.95, 1.05, 48)     # -- Emission Price     [€/t]
    gas = np.ones(48) * 0.03  # * np.random.uniform(0.95, 1.05, 48)    # -- Gas Price          [€/kWh]
    lignite = np.ones(48) * 0.015  # * np.random.uniform(0.95, 1.05)   # -- Lignite Price      [€/kWh]
    coal = np.ones(48) * 0.02  # * np.random.uniform(0.95, 1.05)       # -- Hard Coal Price    [€/kWh]
    nuc = np.ones(48) * 0.01  # * np.random.uniform(0.95, 1.05)        # -- nuclear Price      [€/kWh]

    prices = dict(power=power_price, gas=gas, co=co, lignite=lignite, coal=coal, nuc=nuc)
    prices = pd.DataFrame(data=prices, index=pd.date_range(start='2018-01-01', freq='h', periods=48))

    test_half_power(plant, prices)

    test_ramp_down(plant, prices)

    plant['maxPower'] = 700  # kW
    test_ramp_down(plant, prices)

    plant['minPower'] = 10  # kW
    test_ramp_down(plant, prices)
    plant['maxPower'] = 600  # kW
    test_half_power(plant, prices)

    # test minimize difference
    plant['minPower'] = 10  # kW
    plant['maxPower'] = 600  # kW
    pwp = test_minimize_diff(plant, prices)
    # powerplant runs with minPower
    # currently no evaluation of start cost, if shutdown is possible
    assert pwp.power_plant['on'] == 24
    assert pwp.power_plant['off'] == 0

    plant['minPower'] = 10  # kW
    plant['maxPower'] = 600  # kW
    plant['stopTime'] = 1  # hours
    plant['runTime'] = 1  # hours
    # shut down if possible
    pwp = test_minimize_diff(plant, prices)

    plant['off'] = 3
    plant['on'] = 0
    plant['stopTime'] = 10
    pwp = test_up_down(plant, prices)
