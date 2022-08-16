import numpy as np
import pandas as pd
from pyomo.environ import Var, Objective, SolverFactory, ConcreteModel, NonNegativeReals, Reals, Binary, \
    minimize, quicksum, ConstraintList
from pyomo.environ import value as get_real_number
import time
import logging
from collections import defaultdict


class DayAheadMarket:

    def __init__(self, solver_type: str = 'gurobi', T: int = 24):

        self.logger = logging.getLogger('market')
        self._order_types = ['single_ask',
                             'single_bid', 'linked_ask', 'exclusive_ask']
        self.t = np.arange(T)

        self.orders, self.model_vars = {}, {}
        for order_type in self._order_types:
            self.orders[order_type] = {}
            self.orders[f'{order_type}_index'] = {t: [] for t in self.t}

        self.parent_blocks = {}
        self.agents_with_linked_block = []

        self.model = ConcreteModel()
        if solver_type == 'gurobi':
            self.opt = SolverFactory(solver_type, solver_io='python')
        else:
            self.opt = SolverFactory(solver_type)

    def set_parameter(self, hourly_ask: dict, hourly_bid: dict, linked_orders: dict, exclusive_orders: dict):

        self.orders['single_ask'].update(hourly_ask)
        self.orders['single_bid'].update(hourly_bid)
        self.orders['linked_ask'].update(linked_orders)
        self.orders['exclusive_ask'].update(exclusive_orders)

        for order_type in self._order_types:
            for t in self.t:
                for key_tuple in self.orders[order_type].keys():
                    block, hour, name = key_tuple
                    if hour == t:
                        self.orders[f'{order_type}_index'][t] += [
                            (block, name)]

        for key_tuple, val in self.orders['linked_ask'].items():
            block, _, agent = key_tuple
            self.agents_with_linked_block += [agent]
            _, _, parent_id = val
            child_key = (block, agent)
            self.parent_blocks[child_key] = parent_id

        self.agents_with_linked_block = set(self.agents_with_linked_block)

    def _reset_parameter(self):
        self.orders = {}

        for order_type in self._order_types:
            self.orders[order_type] = {}
            self.orders[f'{order_type}_index'] = {t: [] for t in self.t}

        self.parent_blocks = {}
        self.agents_with_linked_block = []

    def optimize(self):

        self.model.clear()
        self.logger.info('start building model')
        t1 = time.time()
        # Step 1 initialize binary variables for hourly ask block per agent and id
        self.model.use_hourly_ask = Var(set((block, hour, agent) for block, hour, agent
                                            in self.orders['single_ask'].keys()), within=Reals, bounds=(0, 1))
        self.model_vars['single_ask'] = self.model.use_hourly_ask
        # Step 3 initialize binary variables for ask order in block per agent
        self.model.use_linked_order = Var(set([(block, hour, agent) for block, hour, agent
                                               in self.orders['linked_ask'].keys()]), within=Reals, bounds=(0, 1))
        self.model_vars['linked_ask'] = self.model.use_linked_order

        self.model.use_mother_order = Var(
            self.agents_with_linked_block, within=Binary)

        # Step 4 initialize binary variables for exclusive block and agent
        self.model.use_exclusive_block = Var(set([(block, agent) for block, _, agent
                                                  in self.orders['exclusive_ask'].keys()]), within=Binary)
        self.model_vars['exclusive_ask'] = self.model.use_exclusive_block

        self.model.sink = Var(self.t, within=NonNegativeReals)
        self.model.source = Var(self.t, within=NonNegativeReals)

        # Step 6 set constraint: If parent block of an agent is used -> enable usage of child block
        self.model.enable_child_block = ConstraintList()
        self.model.mother_bid = ConstraintList()
        orders = defaultdict(lambda: [])
        for block, hour, agent in self.orders['linked_ask'].keys():
            orders[(block, agent)].append(hour)

        for order, hours in orders.items():
            block, agent = order
            parent_id = self.parent_blocks[block, agent]
            if parent_id != -1:
                if (parent_id, agent) in orders.keys():
                    parent_hours = orders[(parent_id, agent)]
                    self.model.enable_child_block.add(quicksum(self.model.use_linked_order[block, h, agent]
                                                               for h in hours) <=
                                                      2 * quicksum(self.model.use_linked_order[parent_id, h, agent]
                                                                   for h in parent_hours))
                else:
                    self.logger.warning(f'Agent {agent} send invalid linked orders '
                                        f'- block {block} has no parent_id {parent_id}')
                    print('Block, Hour, Agent, Price, Volume, Link')
                    for key, data in self.orders['linked_ask'].items():
                        if key[2] == agent:
                            print(key[0], key[1], key[2],
                                  data[0], data[1], data[2])
            else:
                # mother bid must exist with at least one entry
                # either the whole mother bid can be used or none
                mother_bid_counter = len(hours)
                self.model.mother_bid.add(quicksum(self.model.use_linked_order[0, h, agent] for h in hours)
                                          == mother_bid_counter * self.model.use_mother_order[agent])

        # Constraints for exclusive block orders
        # ------------------------------------------------
        # Step 7 set constraint: only one scheduling can be used
        self.model.one_exclusive_block = ConstraintList()
        for data in set([(agent,) for _, _, agent in self.orders['exclusive_ask'].keys()]):
            agent = data
            self.model.one_exclusive_block.add(1 >= quicksum(
                self.model.use_exclusive_block[:, agent]))

        def get_volume(type_: str, hour: int):
            if type_ != 'single_bid':
                if type_ != 'exclusive_ask':
                    return quicksum(self.orders[type_][block, hour, name][1] *
                                    self.model_vars[type_][block, hour, name]
                                    for block, name in self.orders[f'{type_}_index'][hour])
                else:
                    return quicksum(self.orders[type_][block, hour, name][1] *
                                    self.model_vars[type_][block, name]
                                    for block, name in self.orders[f'{type_}_index'][hour])
            else:
                return quicksum(self.orders[type_][block, hour, name][1]
                                for block, name in self.orders[f'{type_}_index'][hour])

        def get_cost(type_: str, hour: int):
            if type_ != 'single_bid':
                if type_ != 'exclusive_ask':
                    return quicksum(self.orders[type_][block, hour, name][0] *
                                    self.orders[type_][block, hour, name][1] *
                                    self.model_vars[type_][block, hour, name]
                                    for block, name in self.orders[f'{type_}_index'][hour])
                else:
                    return quicksum(-self.orders[type_][block, hour, name][0] *
                                    self.orders[type_][block, hour, name][1] *
                                    self.model_vars[type_][block, name]
                                    for block, name in self.orders[f'{type_}_index'][hour])
            else:
                return quicksum(self.orders[type_][block, hour, name][0] *
                                self.orders[type_][block, hour, name][1]
                                for block, name in self.orders[f'{type_}_index'][hour])

        magic_source = [-1 * quicksum(get_volume(type_=order_type, hour=t) for order_type in self._order_types)
                        for t in self.t]

        # generation must be smaller than demand
        self.model.gen_dem = ConstraintList()
        for t in self.t:
            if not self.orders['single_bid_index'][t]:
                self.logger.error(f'no hourly_bids available at hour {t}')
            elif not (self.orders['single_ask_index'][t] or self.orders['linked_ask_index'][t]):
                # constraints with 0 <= 0 are not valid
                self.logger.error(f'no hourly_asks available at hour {t}')
            else:
                self.model.gen_dem.add(
                    magic_source[t] == self.model.source[t] - self.model.sink[t])

        # Step 9 set constraint: Cost for each hour
        generation_cost = quicksum(quicksum(get_cost(type_=order_type, hour=t) for order_type in self._order_types
                                            if 'bid' not in order_type)
                                   + (self.model.source[t] + self.model.sink[t]) * 1e12 for t in self.t)

        self.model.obj = Objective(expr=generation_cost, sense=minimize)
        self.logger.info(f'built model in {time.time() - t1:.2f} seconds')
        self.logger.info('start optimization/market clearing')
        t1 = time.time()
        try:
            r = self.opt.solve(self.model, options={
                               'MIPGap': 0.1, 'TimeLimit': 60})
            print(r)
        except Exception as e:
            self.logger.exception('error solving optimization problem')
            self.logger.error(f'Model: {self.model}')
            self.logger.error(f'{repr(e)}')
        self.logger.info(f'cleared market in {time.time() - t1:.2f} seconds')

        # -> determine price at each hour
        prices = []
        for t in self.t:
            max_price = - 1000
            for type_ in self.model_vars.keys():
                for block, name in self.orders[f'{type_}_index'][t]:
                    if type_ == 'exclusive_ask':
                        order_used = self.model_vars[type_][block, name].value
                    else:
                        order_used = self.model_vars[type_][block,
                                                            t, name].value
                    if order_used:
                        price = self.orders[type_][block, t, name][0]
                    else:
                        price = - 1000
                    if price > max_price:
                        max_price = price
            prices += [max_price]
        prices = pd.DataFrame(data=dict(price=prices))
        # -> determine volume at each hour
        volumes = []
        sum_magic_source = 0
        for t in self.t:
            sum_magic_source += get_real_number(magic_source[t])
            volume = 0
            for block, name in self.orders['single_bid_index'][t]:
                volume += (-1) * self.orders['single_bid'][block, t, name][1]
            volumes.append(volume)
        self.logger.info(f'Got {sum_magic_source:.2f} kWh from Magic source')
        # -> determine used ask orders
        used_orders = {type_: {} for type_ in self.model_vars.keys()}
        for type_ in self.model_vars.keys():
            for t in self.t:
                for block, name in self.orders[f'{type_}_index'][t]:
                    if type_ in ['single_ask', 'linked_ask']:
                        if self.model_vars[type_][block, t, name].value:
                            f = self.model_vars[type_][block, t, name].value
                            if 'linked' in type_:
                                prc, vol, link = self.orders[type_][block, t, name]
                                vol *= f
                                p = (prc, vol, link)
                            else:
                                prc, vol = self.orders[type_][block, t, name]
                                vol *= f
                                p = (prc, vol)
                            used_orders[type_][(block, t, name)] = p
                    elif type_ == 'exclusive_ask':
                        if self.model_vars[type_][block, name].value:
                            prc, vol = self.orders[type_][block, t, name]
                            used_orders[type_][(block, t, name)] = (prc, vol)

        # -> build dataframe
        for type_ in self.model_vars.keys():
            orders = pd.DataFrame.from_dict(used_orders[type_], orient='index')
            orders.index = pd.MultiIndex.from_tuples(
                orders.index, names=['block_id', 'hour', 'name'])

            if 'linked' in type_ and orders.empty:
                orders['price'] = []
                orders['volume'] = []
                orders['link'] = []
            elif orders.empty:
                orders['price'] = []
                orders['volume'] = []
            elif 'linked' in type_:
                orders.columns = ['price', 'volume', 'link']
            else:
                orders.columns = ['price', 'volume']

            used_orders[type_] = orders.copy()
        # -> return all bid orders
        used_bid_orders = pd.DataFrame.from_dict(
            self.orders['single_bid'], orient='index')
        used_bid_orders.index = pd.MultiIndex.from_tuples(used_bid_orders.index,
                                                          names=['block_id', 'hour', 'name'])
        if used_bid_orders.empty:
            used_bid_orders['price'] = []
            used_bid_orders['volume'] = []
        else:
            used_bid_orders.columns = ['price', 'volume']

        prices['volume'] = volumes
        prices['magic_source'] = [get_real_number(m) for m in magic_source]

        self._reset_parameter()
        return (prices, used_orders['single_ask'],
                used_orders['linked_ask'],
                used_orders['exclusive_ask'],
                used_bid_orders)
