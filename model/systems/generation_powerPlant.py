# third party modules
import numpy as np
import pandas as pd
from pyomo.environ import Constraint, Var, Objective, SolverFactory, ConcreteModel, \
    Reals, Binary, maximize, value, quicksum, ConstraintList
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

        self.committed_power = None

    def set_parameter(self, date, weather=None, prices=None):
        super().set_parameter(date, weather, prices)
        self.base_price = prices.copy()

    def build_model(self):

        self.model.clear()

        delta = self.power_plant['maxPower'] - self.power_plant['minPower']

        self.model.p_out = Var(self.t, bounds=(None, self.power_plant['maxPower']), within=Reals)
        self.model.p_model = Var(self.t, bounds=(0, delta), within=Reals)

        # states (on, ramp up, ramp down)
        self.model.z = Var(self.t, within=Binary)
        self.model.v = Var(self.t, within=Binary)
        self.model.w = Var(self.t, within=Binary)

        # cash flow variables
        # XXX not opt variable - can be pyomo expression
        self.model.fuel = Var(self.t, bounds=(0, None), within=Reals)
        self.model.emissions = Var(self.t, bounds=(0, None), within=Reals)
        self.model.start_ups = Var(self.t, bounds=(0, None), within=Reals)
        self.model.profit = Var(self.t, within=Reals)

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
            if t < 23: # only the next day
                self.model.real_max.add(self.model.p_out[t] <= self.power_plant['minPower']
                                        * (self.model.z[t] + self.model.v[t+1] + self.model.p_model[t]))
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
                self.model.ramping_down.add(self.model.p_model[t-1] - self.model.p_model[t]
                                            <= self.power_plant['gradM'] * self.model.z[t-1])
            # minimal run and stop time
            if t > self.power_plant['stopTime']:
                self.model.stop_time.add(1 - self.model.z[t]
                                         >= quicksum(self.model.w[k] for k in range(t - self.power_plant['stopTime'], t)))
            if t > self.power_plant['runTime']:
                self.model.run_time.add(self.model.z[t]
                                        >= quicksum(self.model.v[k] for k in range(t - self.power_plant['runTime'], t)))
            if t > 0:
                self.model.states.add(self.model.z[t-1] - self.model.z[t] + self.model.v[t] - self.model.w[t] == 0)

            if self.power_plant['on']>0 and t < self.power_plant['runTime'] - self.power_plant['on']:
                self.model.initial_on.add(self.model.z[t] == 1)
            elif self.power_plant['off']>0 and t < self.power_plant['stopTime'] - self.power_plant['off']:
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
        if self.committed_power is None:
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

                self.model.day_ahead_difference.add(self.committed_power[t] - self.model.p_out[t]
                                                    == -self.model.minus[t] + self.model.plus[t])
                self.model.difference_cost.add(self.model.delta_cost[t]
                                               == self.model.power_difference[t] * np.abs(self.prices['power'][t] * 2))

            # set new objective
            self.model.obj = Objective(expr=quicksum(self.model.profit[i] - self.model.fuel[i] - self.model.emissions[i]
                                                     - self.model.start_ups[i] - self.model.delta_cost[i]
                                                     for i in self.t), sense=maximize)

    def optimize(self, steps=None):
        steps = steps or self.steps
        if self.committed_power is None:
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

                if p_out[-1] == 0:
                    all_off = np.argwhere(p_out == 0).flatten()
                    last_on = np.argwhere(p_out > 0).flatten()
                    last_on = last_on[-1] if len(last_on) > 0 else 0
                    hours = all_off[all_off > last_on]

                    self.t = np.arange(48)
                    self.prices = prices_48h
                    self.prices.loc[:, 'power'] = self.base_price.iloc[:48]['power'] + step
                    self.build_model()
                    self.opt.solve(self.model)
                    power_check = np.asarray([self.model.p_out[t].value for t in self.t])
                    prevent_start = all(power_check[hours] > 0)
                    delta = value(self.model.obj) - objective_value
                    percentage = delta / objective_value if objective_value else 0
                    if prevent_start and percentage > 0.05:
                        self.prevented_start = dict(prevent=True, hours=hours, delta=delta/len(hours))
                    self.t = np.arange(self.T)

                if step == 0:
                    self.cash_flow = dict(fuel=self.optimization_results[step]['fuel'],
                                          emission=self.optimization_results[step]['emission'],
                                          start_ups=self.optimization_results[step]['start'],
                                          profit=self.optimization_results[step]['profit'])
                    self.generation[str(self.power_plant['fuel']).replace('_combined', '')] = self.optimization_results[step]['power']
                    self.power = self.optimization_results[step]['power']

        else:
            self.build_model()
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
                if self.model.z[t].value >0:
                    running_since += 1
                    off_since = 0
                else:
                    running_since = 0
                    off_since +=1

            self.power_plant['P0'] = self.power[-1]
            self.power_plant['on'] = running_since
            self.power_plant['off'] = off_since

            self.committed_power = None

        return self.power.copy()

    def get_clean_spread(self, prices=None):
        prices = prices or self.prices
        return 1/self.power_plant['eta'] * (prices[self.power_plant['fuel']].mean()
                                            + self.power_plant['chi'] * prices['co'].mean())

    def get_orderbook(self) -> pd.DataFrame:
        order_counter = 5

        def set_order(r: dict, h: int, block_nr: int, split: int, link: dict,
                      lst_pwr: np.array = np.zeros(self.T), add: int = 0):
            book = {}
            pwr = r['power'][h]
            if split == 0:
                prc = np.mean((r['fuel'][h] + r['emission'][h] + r['start'][h]) / pwr)
                l = -1 if link[h] is None else link[h]
                book[(block_nr, h, 0, self.name)] = (prc, pwr - lst_pwr[h], l)
            else:
                prc = (r['fuel'][h] + r['emission'][h]) / pwr
                l = 0 if link[h + add] is None else link[h + add]
                for o in range(split):
                    book[(block_nr, h, o, self.name)] = (prc, (pwr - lst_pwr[h]) / split, l)
            return book

        # -> initialize empty dict
        order_book = {}
        last_power = np.zeros(self.T)
        block_number = 0
        links = {i: None for i in self.t}           # -> links[hour] = last_block

        for step in self.steps:

            # -> get optimization result for key (block) and step
            result = self.optimization_results[step]
            if any(result['power'] > 0) and block_number == 0:
                # so lange wie ich muss
                hours_needed_to_run = (self.power_plant['runTime'] - self.power_plant['on'])

                if hours_needed_to_run < 1:
                    order_book.update(set_order(result, 0, block_number, order_counter, links))
                    links[0] = block_number
                    last_power[0] = result['power'][0]                    # -> set last_power to current power
                else:
                    for hour in np.argwhere(result['power'] > 0).flatten():
                        order_book.update(set_order(result, hour, block_number, 0, links))
                        links[hour] = block_number
                        last_power[hour] = result['power'][hour]                    # -> set last_power to current power
                block_number += 1                               # -> increment block number



                if self.prevented_start['prevent']:
                    for hour in self.prevented_start['hours']:
                        cost = result['fuel'][hour] + result['emission'][hour] / self.power_plant['minPower']
                        prc = cost - (self.prevented_start['delta'] / self.power_plant['minPower'])
                        order_book[(block_number, hour, 0, self.name)] = (prc, self.power_plant['minPower'], 0)
                        last_power[hour] += self.power_plant['minPower']
                        links[hour] = block_number
                    block_number += 1

                continue  # do next step

            # -> add linked hour blocks
            # -> check if current power is higher then the last known power
            if any(result['power'] - last_power > 0):
                delta = result['power'] - last_power
                # -> add on top
                for hour in self.t:
                    if delta[hour] > 0 and last_power[hour] > 0:
                        order_book.update(set_order(result, hour, block_number, order_counter, links, last_power))
                        last_power[hour] += delta[hour]
                        links[hour] = block_number          # -> update last known block for hour
                        block_number += 1                   # -> increment block number
                # -> add left and right
                stack = np.argwhere(last_power > 0).flatten()
                if len(stack) > 0:
                    delta = result['power'] - last_power
                    for start, end, inc in zip([stack[-1], stack[0]], [self.T, 0], [1, -1]):
                        for hour in range(start, end, inc):
                            if delta[hour] > 0:
                                order_book.update(set_order(result, hour, block_number, order_counter, links, last_power, -inc))
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
        df.index = pd.MultiIndex.from_tuples(df.index, names=['block_id', 'hour', 'order_id', 'name'])

        return df


def visualize_orderbook(order_book):
    import matplotlib.pyplot as plt
    from matplotlib.colors import ListedColormap
    tab20_cmap = plt.get_cmap("tab20c")
    ob = order_book.reset_index( level = [0,2,3])
    idx = np.arange(24)

    y_past = np.zeros(24)
    for i, df_grouped in ob.groupby('order_id'):
        my_cmap_raw = np.array(tab20_cmap.colors)*(1-0.1*i)
        my_cmap = ListedColormap(my_cmap_raw)

        for j, o in df_grouped.groupby('link'):
            x = idx #o.index
            ys = np.zeros(24)
            ys[o.index] = o['volume']
            if len(list(ys)) > 0:
                plt.bar(x, ys, bottom=y_past, color=my_cmap.colors[(j+1)%20])
            y_past += ys
    plt.title('Orderbook')
    plt.xlabel('hour')
    plt.ylabel('kW')
    plt.show()

def test_half_power(plant):
    pwp = PowerPlant(T=24, steps=steps, **plant)
    pwp.set_parameter(date='2018-01-01', weather=None,
                    prices=prices)

    power = pwp.optimize()
    o_book = pwp.get_orderbook()
    # running since 1
    visualize_orderbook(o_book)

    #clean_spread = (1/plant['eta'] * (prices[plant['fuel']].mean() + plant['chi'] * prices['co'].mean()))
    print(f'{pwp.get_clean_spread()} €/kWh cost')
    print(f"{pwp.power_plant['maxPower']*pwp.get_clean_spread()} €/h full operation")

    # assume market only gives you halve of your offers
    pwp.committed_power = power/2
    pwp.optimize()

    # running since 1
    visualize_orderbook(pwp.get_orderbook())

    assert all((power/2 - pwp.power) < 1e-10) # smaller than threshold
    assert pwp.power_plant['on'] == 24
    assert pwp.power_plant['off'] == 0

def test_ramp_down(plant):
    pwp = PowerPlant(T=24, steps=steps, **plant)
    pwp.set_parameter(date='2018-01-01', weather=None,
                    prices=prices)
    power = pwp.optimize()

    visualize_orderbook(pwp.get_orderbook())

    # power plant should ramp down correctly
    pwp.committed_power = power*0

    pwp.optimize()
    visualize_orderbook(pwp.get_orderbook())

    assert pwp.power_plant['on'] == 0
    assert pwp.power_plant['off'] == 19

    pwp.set_parameter(date='2018-01-02', weather=None,
                    prices=prices)

    pwp.optimize()

    visualize_orderbook(pwp.get_orderbook())

    # another day off - this time a full day
    pwp.committed_power = power*0
    pwp.optimize()

    visualize_orderbook(pwp.get_orderbook())

    assert pwp.power_plant['on'] == 0
    assert pwp.power_plant['off'] == 24

    #for k,v in pwp.optimization_results.items(): print(k, v['power'])
    #for k,v in pwp.optimization_results.items(): print(k, v['obj'])
    # actual schedule corresponds to the market result

def test_minimize_diff(plant):
    pwp = PowerPlant(T=24, steps=steps, **plant)
    pwp.set_parameter(date='2018-01-01', weather=None,
                    prices=prices)
    power = pwp.optimize()

    visualize_orderbook(pwp.get_orderbook())

    # power plant has to minimize loss, when market did something weird
    p = power.copy()
    p[4:10] = 0
    pwp.committed_power = p

    p2 = pwp.optimize()
    visualize_orderbook(pwp.get_orderbook())

    return pwp

def test_up_down(plant):
    pwp = PowerPlant(T=24, steps=steps, **plant)
    pwp.set_parameter(date='2018-01-01', weather=None,
                    prices=prices)
    power = pwp.optimize()

    visualize_orderbook(pwp.get_orderbook())

    # power plant has to minimize loss, when market did something weird
    p = power.copy()
    p[::2] = 0
    pwp.committed_power = p

    p2 = pwp.optimize()
    visualize_orderbook(pwp.get_orderbook())
    return pwp

if __name__ == "__main__":
    plant = {'unitID':'x',
             'fuel':'lignite',
             'maxPower': 300, # kW
             'minPower': 100, # kW
             'eta': 0.4, # Wirkungsgrad
             'P0': 120,
             'chi': 0.407/1e3, # t CO2/kWh
             'stopTime': 12, # hours
             'runTime': 6, # hours
             'gradP': 300, # kW/h
             'gradM': 300, # kW/h
             'on': 1, # running since
             'off': 0,
             'startCost': 1e3 # €/Start
             }
    steps = np.array([-100, 0, 100])

    power_price = np.ones(48) #* np.random.uniform(0.95, 1.05, 48) # €/kWh
    co = np.ones(48) * 23.8 #* np.random.uniform(0.95, 1.05, 48)     # -- Emission Price     [€/t]
    gas = np.ones(48) * 0.03 #* np.random.uniform(0.95, 1.05, 48)    # -- Gas Price          [€/kWh]
    lignite = np.ones(48) * 0.015 #* np.random.uniform(0.95, 1.05)   # -- Lignite Price      [€/kWh]
    coal = np.ones(48) * 0.02 #* np.random.uniform(0.95, 1.05)       # -- Hard Coal Price    [€/kWh]
    nuc = np.ones(48) * 0.01 #* np.random.uniform(0.95, 1.05)        # -- nuclear Price      [€/kWh]

    prices = dict(power=power_price, gas=gas, co=co, lignite=lignite, coal=coal, nuc=nuc)
    prices = pd.DataFrame(data=prices, index=pd.date_range(start='2018-01-01', freq='h', periods=48))

    test_half_power(plant)

    test_ramp_down(plant)

    plant['maxPower'] = 700 # kW
    test_ramp_down(plant)

    plant['minPower'] = 10 # kW
    test_ramp_down(plant)
    plant['maxPower'] = 600 # kW
    test_half_power(plant)

    # test minimize difference
    plant['minPower'] = 10 # kW
    plant['maxPower'] = 600 # kW
    pwp = test_minimize_diff(plant)
    # powerplant runs with minPower
    # currently no evaluation of start cost, if shutdown is possible
    assert pwp.power_plant['on'] == 24
    assert pwp.power_plant['off'] == 0

    plant['minPower'] = 10 # kW
    plant['maxPower'] = 600 # kW
    plant['stopTime'] = 1 # hours
    plant['runTime'] = 1 # hours
    # shut down if possible
    pwp = test_minimize_diff(plant)

    plant['off'] = 3
    plant['on'] = 0
    plant['stopTime'] = 10
    pwp = test_up_down(plant)
