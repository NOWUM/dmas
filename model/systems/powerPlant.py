# third party modules
import logging
from datetime import timedelta as td
import numpy as np
import pandas as pd
from pyomo.environ import (Binary, ConcreteModel, Constraint, ConstraintList,
                           NonNegativeReals, Objective, Reals, SolverFactory,
                           Var, maximize, quicksum, value)
from pyomo.opt import SolverStatus, TerminationCondition

# model modules
from systems.basic_system import EnergySystem

log = logging.getLogger('powerplant')


class PowerPlant(EnergySystem):

    def __init__(self, T, steps: tuple, unitID: str, fuel: str, maxPower: float, minPower: float,
                 eta: float, P0: float, chi: float, stopTime: int, runTime: int, gradP: float, gradM: float,
                 on: int, off: int, startCost: float, *args, **kwargs):
        super().__init__(T=T, fuel_type=fuel)

        self.name = unitID
        self.base_price = None

        # PWP solves in kW as all others too
        self.generation_system = dict(fuel=fuel, maxPower=maxPower, minPower=minPower, eta=eta, P0=P0, chi=chi,
                                      stopTime=stopTime, runTime=runTime, gradP=gradP, gradM=gradM, on=on, off=off)
        self.start_cost = startCost

        self.model = ConcreteModel()
        self.opt = SolverFactory('glpk')

        self.steps = steps
        self.opt_results = {step: dict(power=np.zeros(self.T, float),
                                       emission=np.zeros(self.T, float),
                                       fuel=np.zeros(self.T, float),
                                       start=np.zeros(self.T, float),
                                       profit=np.zeros(self.T, float),
                                       obj=0) for step in steps}

        self.prevented_start = dict(prevent=False, hours=np.zeros(self.T, float), delta=0)
        self.reduction_next_day = dict()


    def set_parameter(self, date: pd.Timestamp, weather: pd.DataFrame = None, prices: pd.DataFrame = None) -> None:
        super()._set_parameter(date, weather, prices)
        self.base_price = prices.copy()

    def build_model(self, committed_power: np.array = None) -> None:
        pwp = self.generation_system

        self.model.clear()

        delta = pwp['maxPower'] - pwp['minPower']

        self.model.p_out = Var(self.t, bounds=(0, pwp['maxPower']), within=Reals)
        self.model.p_model = Var(self.t, bounds=(0, delta), within=Reals)

        # states (on, ramp up, ramp down)
        self.model.z = Var(self.t, within=Binary)
        self.model.v = Var(self.t, within=Binary)
        self.model.w = Var(self.t, within=Binary)

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

        try:
            fuel_prices = self.prices[str(pwp['fuel']).replace('_combined', '')].values
            emission_prices = self.prices['co'].values
            power_prices = self.prices['power'].values
        except KeyError:
            log.error(f'prices were: {self.prices}')
            raise Exception(f"No Fuel prices given for fuel {pwp['fuel']}")

        for t in self.t:
            # output power of the plant
            self.model.real_power.add(self.model.p_out[t] == self.model.p_model[t] + self.model.z[t] * pwp['minPower'])
            if t < 23:  # only the next day
                self.model.real_max.add(self.model.p_out[t] <= pwp['minPower']
                                        * (self.model.z[t] + self.model.v[t + 1] + self.model.p_model[t]))
            # model power for optimization
            self.model.model_min.add(0 <= self.model.p_model[t])
            self.model.model_max.add(self.model.z[t] * delta >= self.model.p_model[t])
            # ramping (gradients)
            if t == 0:
                self.model.ramping_up_0 = Constraint(expr=self.model.p_out[0] <= pwp['P0'] + pwp['gradP'])
                self.model.ramping_down_0 = Constraint(expr=self.model.p_out[0] >= pwp['P0'] - pwp['gradM'])
            else:
                self.model.ramping_up.add(self.model.p_model[t] - self.model.p_model[t - 1]
                                          <= pwp['gradP'] * self.model.z[t - 1])
                self.model.ramping_down.add(self.model.p_model[t - 1] - self.model.p_model[t]
                                            <= pwp['gradM'] * self.model.z[t])
            # minimal run and stop time
            if t > pwp['stopTime']:
                self.model.stop_time.add(1 - self.model.z[t]
                                         >= quicksum(self.model.w[k] for k in range(t - pwp['stopTime'], t)))
            if t > pwp['runTime']:
                self.model.run_time.add(self.model.z[t]
                                        >= quicksum(self.model.v[k] for k in range(t - pwp['runTime'], t)))
            if t > 0:
                self.model.states.add(self.model.z[t - 1] - self.model.z[t] + self.model.v[t] - self.model.w[t] == 0)

            if pwp['on'] > 0 and t < pwp['runTime'] - pwp['on']:
                self.model.initial_on.add(self.model.z[t] == 1)
            elif pwp['off'] > 0 and t < pwp['stopTime'] - pwp['off']:
                self.model.initial_off.add(self.model.z[t] == 0)

        # -> fuel costs
        fuel_cost = [(self.model.p_out[t] / pwp['eta']) * fuel_prices[t] for t in self.t]
        # -> emission costs
        emission_cost = [(self.model.p_out[t] / pwp['eta'] * pwp['chi']) * emission_prices[t]
                         for t in self.t]
        # -> start costs
        start_cost = [self.model.v[t] * self.start_cost for t in self.t]

        # -> profit and resulting cashflow
        profit = [self.model.p_out[t] * power_prices[t] for t in self.t]
        cashflow = [profit[t] - (fuel_cost[t] + emission_cost[t] + start_cost[t]) for t in self.t]

        # if no day ahead power known run standard optimization
        if committed_power is None:
            self.model.obj = Objective(expr=quicksum(cashflow), sense=maximize)
        # if day ahead power is known minimize the difference
        else:
            self.model.power_difference = Var(self.t, within=NonNegativeReals)
            self.model.minus = Var(self.t, within=NonNegativeReals)
            self.model.plus = Var(self.t, within=NonNegativeReals)

            difference = [self.model.minus[t] + self.model.plus[t] for t in self.t]
            self.model.difference = ConstraintList()
            for t in self.t:
                self.model.difference.add(committed_power[t] - self.model.p_out[t]
                                          == -self.model.minus[t] + self.model.plus[t])
            difference_cost = [difference[t] * np.abs(self.prices['power'].iloc[t] * 2) for t in self.t]

            # set new objective
            self.model.obj = Objective(expr=quicksum(cashflow[t] - difference_cost[t] for t in self.t), sense=maximize)

    def _set_results(self, step: int) -> None:
        pwp = self.generation_system
        # -> output power
        power = np.asarray([self.model.p_out[t].value for t in self.t])
        self.opt_results[step]['power'] = power
        self.opt_results[step]['power'][power < 0.1] = 0

        # -> emission costs
        em_prices = self.prices['co'].values
        self.opt_results[step]['emission'] = power / pwp['eta'] * pwp['chi'] * em_prices
        # -> fuel costs
        fl_prices = self.prices[str(pwp['fuel']).replace('_combined', '')].values
        self.opt_results[step]['fuel'] = power / pwp['eta'] * fl_prices
        # -> start costs
        self.opt_results[step]['start'] = np.asarray([self.model.v[t].value * self.start_cost for t in self.t])
        # -> profit
        pw_prices = self.prices['power'].values
        self.opt_results[step]['profit'] = pw_prices * power
        # -> sum cashflow
        self.opt_results[step]['obj'] = value(self.model.obj)

        if step == 0:
            self.cash_flow['fuel'] = self.opt_results[step]['fuel']
            self.cash_flow['emission'] = self.opt_results[step]['emission']
            self.cash_flow['start_ups'] = self.opt_results[step]['start']
            self.cash_flow['profit'] = self.opt_results[step]['profit']
            self.generation[str(pwp['fuel']).replace('_combined', '')] = self.opt_results[step]['power']
            self.generation['total'] = self.opt_results[step]['power']
            self.power = self.opt_results[step]['power']

    def optimize(self, date: pd.Timestamp = None, weather: pd.DataFrame = None, prices: pd.DataFrame=None,
                 steps: tuple = None) -> np.array:
        pwp = self.generation_system

        self.set_parameter(date, weather, prices)
        self.prevented_start = dict(prevent=False, hours=np.zeros(self.T, float), delta=0)
        steps = steps or self.steps
        prices_24h = self.base_price.iloc[:24, :].copy()
        prices_48h = self.base_price.iloc[:48, :].copy()

        for step in steps:
            self.prices = prices_24h
            self.prices.loc[:, 'power'] = self.base_price.iloc[:24]['power'] + step
            self.build_model()
            r = self.opt.solve(self.model)
            if (r.solver.status == SolverStatus.ok) & (r.solver.termination_condition == TerminationCondition.optimal):
                log.info(f'find optimal solution in step: {step}')

                self._set_results(step=step)

                if self.opt_results[step]['power'][-1] == 0 and step == 0:
                    initial_p0 = self.generation_system['P0']
                    initial_on = self.generation_system['on']
                    initial_off = self.generation_system['off']
                    all_off = np.argwhere(self.opt_results[step]['power'] == 0).flatten()
                    last_on = np.argwhere(self.opt_results[step]['power'] > 0).flatten()
                    last_on = last_on[-1] if len(last_on) > 0 else 0
                    prevented_off_hours = list(all_off[all_off > last_on])
                    self.prices.loc[:, 'power'] = self.base_price['power'].values[24:]

                    self.generation_system['P0'] = 0
                    self.generation_system['off'] = len(prevented_off_hours)
                    self.generation_system['on'] = 0
                    self.build_model()
                    self.opt.solve(self.model)
                    total_obj_single = self.opt_results[step]['obj'] + value(self.model.obj)
                    power_day1 = list(self.opt_results[step]['power'])
                    power_day2 = [self.model.p_out[t].value for t in self.t]
                    total_single_power = np.asarray(power_day1 + power_day2)

                    all_off = np.argwhere(total_single_power == 0).flatten()
                    prevented_off_hours = np.asarray(list(all_off[all_off > last_on]))

                    self.generation_system['P0'] = initial_p0
                    self.generation_system['off'] = initial_off
                    self.generation_system['on'] = initial_on

                    self.t = np.arange(48)
                    self.prices = prices_48h
                    self.prices.loc[:, 'power'] = self.base_price.iloc[:48]['power']
                    self.build_model()
                    self.opt.solve(self.model)
                    power_check = np.asarray([self.model.p_out[t].value for t in self.t])
                    prevent_start = all(power_check[prevented_off_hours] > 0)
                    delta = value(self.model.obj) - total_obj_single
                    if prevent_start and delta > 0:
                        delta /= sum(power_check[prevented_off_hours])
                        prevent_start_today = prevented_off_hours[prevented_off_hours < self.T]
                        self.prevented_start = dict(prevent=True, hours=prevent_start_today, delta=delta)
                        prevent_start_tomorrow = prevented_off_hours[prevented_off_hours >= self.T] - self.T
                        self.reduction_next_day[self.date.date()] = (delta, prevent_start_tomorrow)

                    self.t = np.arange(self.T)

            elif r.solver.termination_condition == TerminationCondition.infeasible:
                log.error(f'infeasible model in step: {step}')
                for key in ['power', 'emission', 'fuel', 'start', 'profit']:
                    self.opt_results[step][key] = np.zeros(self.T)
                self.opt_results[step]['obj'] = 0
            else:
                print(step)
                log.error(r.solver)
                for key in ['power', 'emission', 'fuel', 'start', 'profit']:
                    self.opt_results[step][key] = np.zeros(self.T)
                self.opt_results[step]['obj'] = 0

        return self.power

    def optimize_post_market(self, committed_power: np.array, power_prices: np.array = None) -> np.array:
        if power_prices is not None:
            self.prices['power'].values[:len(power_prices)] = power_prices
        self.build_model(committed_power)
        r = self.opt.solve(self.model)

        if (r.solver.status == SolverStatus.ok) & (r.solver.termination_condition == TerminationCondition.optimal):
            log.info(f'find optimal solution in step: dayAhead adjustment')
            self._set_results(step=0)
            running_since, off_since = 0, 0
            for t in self.t:
                # find count of last 1s and 0s
                if self.model.z[t].value > 0:
                    running_since += 1
                    off_since = 0
                else:
                    running_since = 0
                    off_since += 1

            last_power = self.power[-1]

        elif r.solver.termination_condition == TerminationCondition.infeasible:
            log.error(f'infeasible model in step: dayAhead adjustment')
            running_since, off_since = 1, 0
            last_power = self.generation_system['minPower']
        else:
            log.error(r.solver)
            running_since, off_since = 1, 0
            last_power = self.generation_system['minPower']

        self.generation_system['P0'] = last_power
        self.generation_system['on'] = running_since
        self.generation_system['off'] = off_since

        return self.power.copy()

    def get_clean_spread(self, prices: pd.DataFrame = None) -> float:
        pwp, prices = self.generation_system, prices or self.prices
        return 1 / pwp['eta'] * (prices[pwp['fuel'].replace('_combined', '')].mean() + pwp['chi'] * prices['co'].mean())

    def get_ask_orders(self, price: float = -0.5) -> pd.DataFrame:

        def get_cost(p: float, t: int):
            f = self.prices[self.generation_system['fuel'].replace('_combined', '')].values[t]
            e = self.prices['co'].values[t]
            return (p / self.generation_system['eta']) * (f + e * self.generation_system['chi'])

        def get_marginal(p0: float, p1: float, t: int):
            if p0 == p1:
                return 1, 0
            marginal = (get_cost(p=p0, t=t) - get_cost(p=p1, t=t)) / (p0-p1)
            return marginal, p1-p0

        def get_maximal_profit_hours():
            max_profit, start_hour = 0, 0
            run_time = self.generation_system['runTime']
            for t in range(self.T - run_time):
                p = np.sum(self.generation_system['minPower'] * self.base_price['power'][t:t + run_time])
                if p > max_profit:
                    max_profit = p
                    start_hour = t
            return [*range(start_hour, min(start_hour + run_time, self.T))]

        order_book, last_power, block_number = {}, np.zeros(self.T), 0
        links = {i: None for i in self.t}

        min_power = self.generation_system['minPower']
        max_hours = get_maximal_profit_hours()
        start_cost = self.start_cost / (min_power * min_power)

        yesterday = self.date.date() - td(days=1)

        index = 0

        while index < len(self.steps):
            step = self.steps[index]

            # -> get optimization result for key (block) and step
            result = self.opt_results[step]
            # if we are in hour 0
            if any(result['power'] > 0) and block_number == 0:
                # pwp is on and must runtime is reached
                if result['power'][0] > 0 and self.generation_system['P0'] > 0:
                    reduction = 0
                    hours_needed_to_run = (self.generation_system['runTime'] - self.generation_system['on'])
                    hours = [*range(hours_needed_to_run)] if hours_needed_to_run > 0 else [0]
                    # -> a start is prevented
                    if yesterday in self.reduction_next_day.keys():
                        reduction, hours = self.reduction_next_day[yesterday]
                        self.reduction_next_day = dict()
                elif self.generation_system['P0'] == 0:
                    # pwp is off
                    hours = max_hours
                    reduction = -start_cost
                else:
                    # pwp was on but turned off in first hour
                    hours = max_hours
                    reduction = -start_cost

                for hour in hours:
                    price, power = get_marginal(p0=last_power[hour], p1=min_power, t=hour)
                    order_book.update({(block_number, hour, self.name): (price - reduction, power, -1)})
                    links[hour] = block_number
                    last_power[hour] += min_power

                block_number += 1  # -> increment block number

            # -> stack on top
            # XXX bitwise and operator
            hours = np.argwhere((result['power'] - last_power > 0.1) & (last_power > 0)).flatten()
            for hour in hours:
                price, power = get_marginal(p0=last_power[hour], p1=result['power'][hour], t=hour)
                order_book.update({(block_number, hour, self.name): (price, power, links[hour])})
                last_power[hour] += power
                links[hour] = block_number
                block_number += 1
            # -> stack before
            hours = np.argwhere((result['power'] - last_power > 0.1) & (last_power == 0)).flatten()
            first_on_hour = np.argwhere(last_power == 0)[-1][0] + 1 if len(np.argwhere(last_power == 0)) else 0
            first_on_hour = 0 if first_on_hour > 23 else first_on_hour
            first_hours = list(hours[hours < first_on_hour])
            # -> no gap between current (mother) block and new block on the left side
            if first_hours:
                set_hours = []
                delta_hour = first_on_hour - first_hours[-1]
                if delta_hour == 1:
                    first_hours.reverse()
                    for hour in first_hours:
                        if links[hour+1] is not None:
                            price, power = get_marginal(p0=last_power[hour], p1=result['power'][hour], t=hour)
                            order_book.update({(block_number, hour, self.name): (price, power, links[hour+1])})
                            last_power[hour] += power
                            links[hour] = block_number
                            block_number += 1
                            set_hours += [hour]
                        else:
                            break
                first_hours = list(set(first_hours) - set(set_hours))
                if delta_hour > 1 or len(first_hours) > 0:
                    total_start_cost = result['start'][first_hours[0]]
                    result['start'][first_hours] = total_start_cost / (self.generation_system['minPower'] *
                                                                       len(first_hours))
                    # -> add new mother block before another mother block
                    # -> this means that a new start is added before a start in a previous step
                    for hour in first_hours:
                        price, power = get_marginal(p0=last_power[hour], p1=self.generation_system['minPower'], t=hour)
                        order_book.update({(block_number, hour, self.name): (price, power, -1)})
                        last_power[hour] += self.generation_system['minPower']
                        links[hour] = block_number
                    block_number += 1

                    for hour in first_hours:
                        if result['power'][hour] > last_power[hour]:
                            price, power = get_marginal(p0=last_power[hour], p1=result['power'][hour], t=hour)
                            order_book.update({(block_number, hour, self.name): (price, power, links[hour - 1])})
                            last_power[hour] += power
                            links[hour] = block_number
                            block_number += 1

            # -> stack behind
            last_on_hour = np.argwhere(last_power > 0)[-1][0] if len(np.argwhere(last_power > 0)) else self.t[-1]
            last_on_hours = list(hours[hours > last_on_hour])
            for hour in last_on_hours:
                if links[hour-1] is None:
                    # we need to start mid day
                    if all(result['power'][max_hours] > 0):
                        # pwp is on and must runtime is not reached or it is turned off and started later
                        for t in max_hours:
                            price, power = get_marginal(p0=last_power[t], p1=min_power, t=t)
                            order_book.update({(block_number, t, self.name): (price + start_cost, power, -1)})
                            links[t] = block_number
                            last_power[t] += min_power
                    block_number += 1
                    index -= 1
                    break
                else:
                    if result['power'][hour] > last_power[hour]:
                        price, power = get_marginal(p0=last_power[hour], p1=result['power'][hour], t=hour)
                        order_book.update({(block_number, hour, self.name): (price, power, links[hour-1])})
                        last_power[hour] += power
                        links[hour] = block_number
                        block_number += 1
            index += 1
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

            def get_marginals(x):
                return get_marginal(p0=0, p1=self.generation_system['minPower'], t=x)
            min_price = np.mean([price for price, _ in map(get_marginals, hours)]) - self.prevented_start['delta']

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
                prev_order[(block_number, hour, self.name)] = (min_price, self.generation_system['minPower'],
                                                               last_block, 'generation')
            last_block = block_number
            block_number += 1
            for index in normal_volume.index:
                vol = normal_volume.loc[index] - self.generation_system['minPower']
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

    def __str__(self):
        status = f'power plant: {self.generation_system}, \n\n' \
                 f'prevented start: {self.prevented_start} \n\n'

        status += 'optimization results: \n'

        for key, value in self.opt_results.items():
            status += f'Optimization Result for Step: {key}: \n ' \
                      f'power: {np.round(value["power"], 2)}, \n ' \
                      f'emission: {np.round(value["emission"], 2)}, \n ' \
                      f'fuel: {np.round(value["fuel"], 2)}, \n ' \
                      f'start: {np.round(value["start"], 2)} \n\n ' \

        return status


if __name__ == "__main__":
    from systems.utils import get_test_power_plant, get_test_prices, visualize_orderbook

    with open(r'./forecasts/data/default_price.pkl', 'rb') as file:
        # load default prices in €/MWh elek -> convert to €/kWh
        default_power_price = np.load(file).reshape((24,))

    plant = get_test_power_plant()
    plant['on'] = 0
    plant['off'] = 5
    plant['P0'] = 0
    plant['runTime'], plant['stopTime'] = 3, 8
    steps = (-100, -1, 0, 6)
    power_plant = PowerPlant(T=24, steps=steps, **plant)
    prices = get_test_prices(num=48)
    prices['power'] = np.asarray(2*[default_power_price]).flatten()

    # prices['power'].values[:6] = -400
    # prices['power'].values[6:12] = -50
    # prices['power'].values[12:] = 10
    prices['power'].values[18:] = -10
    prices['power'].values[24:] = 100

    power_plant.optimize(date=pd.Timestamp(2018, 1, 1), prices=prices, weather=pd.DataFrame())
    order_book = power_plant.get_ask_orders()
    # visualize_orderbook(order_book)
    # print(power_plant)
    comm_power = list(order_book.groupby(order_book.index.get_level_values('hour')).sum()['volume'].values)
    comm_power = np.asarray(7*[0] + comm_power)
    power_plant.optimize_post_market(comm_power, prices['power'].values[:24])
    prices = prices.loc[prices.index >= pd.Timestamp(2018, 1, 2)]
    pr = prices.copy()
    pr.index = pd.date_range(start='2018-01-03', periods=24, freq='h')
    prices = pd.concat([prices, pr])

    power_plant.optimize(date=pd.Timestamp(2018, 1, 2), prices=prices, weather=pd.DataFrame())
    order_book = power_plant.get_ask_orders()
    visualize_orderbook(order_book)