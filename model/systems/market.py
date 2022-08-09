import numpy as np
import pandas as pd
from pyomo.environ import Var, Objective, SolverFactory, ConcreteModel, Reals, Binary, \
    minimize, quicksum, ConstraintList
from pyomo.environ import value as get_real_number
import time
import logging
from collections import defaultdict


class DayAheadMarket:

    def __init__(self, solver_type: str = 'gurobi', T: int = 24):

        self.logger = logging.getLogger('market')
        self._order_types = ['single_ask', 'single_bid', 'linked_ask', 'exclusive_ask']
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
                        self.orders[f'{order_type}_index'][t] += [(block, name)]

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

        self.model.use_mother_order = Var(self.agents_with_linked_block, within=Binary)

        # Step 4 initialize binary variables for exclusive block and agent
        self.model.use_exclusive_block = Var(set([(block, agent) for block, _, agent
                                                  in self.orders['exclusive_ask'].keys()]), within=Binary)
        self.model_vars['exclusive_ask'] = self.model.use_exclusive_block

        self.model.sink = Var(self.t, within=Reals, bounds=(0, None))
        self.model.source = Var(self.t, within=Reals, bounds=(0, None))

        # Step 6 set constraint: If parent block of an agent is used -> enable usage of child block
        self.model.enable_child_block = ConstraintList()
        self.model.mother_bid = ConstraintList()
        orders = defaultdict(lambda: [])
        for block, hour, agent in self.orders['linked_ask'].keys():
            orders[(block, agent)].append(hour)

        print(orders)

        for order, hours in orders.items():
            print('hours:', hours)
            print('order:', order)
            block, agent = order
            parent_id = self.parent_blocks[block, agent]
            if parent_id != -1:
                parent_hours = orders[(parent_id, agent)]
                print('parent_id:', parent_hours)
                self.model.enable_child_block.add(quicksum(self.model.use_linked_order[block, h, agent] for h in hours) <=
                                                  2 * quicksum(self.model.use_linked_order[parent_id, h, agent] for h in parent_hours))
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
        for data in set([(agent,) for _, _, _, agent in self.orders['exclusive_ask'].keys()]):
            agent = data
            self.model.one_exclusive_block.add(1 >= quicksum(self.model.use_exclusive_block[:, agent]))

        def get_volume(type_: str, hour: int):
            if type_ != 'single_bid':
                return quicksum(self.orders[type_][block, hour, name][1] *
                                self.model_vars[type_][block, hour, name]
                                for block, name in self.orders[f'{type_}_index'][hour])
            else:
                return quicksum(self.orders[type_][block, hour, name][1]
                                for block, name in self.orders[f'{type_}_index'][hour])

        def get_cost(type_: str, hour: int):
            if type_ != 'single_bid':
                return quicksum(self.orders[type_][block, hour, name][0] *
                                self.orders[type_][block, hour, name][1] *
                                self.model_vars[type_][block, hour, name]
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
                self.model.gen_dem.add(magic_source[t] == self.model.source[t] - self.model.sink[t])

        # Step 9 set constraint: Cost for each hour
        generation_cost = quicksum(quicksum(get_cost(type_=order_type, hour=t) for order_type in self._order_types
                                            if 'bid' not in order_type)
                                   + (self.model.source[t] + self.model.sink[t]) * 1e12 for t in self.t)

        self.model.obj = Objective(expr=generation_cost, sense=minimize)
        self.logger.info(f'built model in {time.time() - t1:.2f} seconds')
        self.logger.info(f'start optimization/market clearing')
        t1 = time.time()
        try:
            r = self.opt.solve(self.model, options={'MIPGap': 0.1, 'TimeLimit': 60})
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
                    if self.model_vars[type_][block, t, name].value:
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
        # -> build dataframe
        for type_ in self.model_vars.keys():
            orders = pd.DataFrame.from_dict(used_orders[type_], orient='index')
            orders.index = pd.MultiIndex.from_tuples(orders.index, names=['block_id', 'hour', 'name'])

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
        used_bid_orders = pd.DataFrame.from_dict(self.orders['single_bid'], orient='index')
        used_bid_orders.index = pd.MultiIndex.from_tuples(used_bid_orders.index,
                                                          names=['block_id', 'hour', 'name'])
        if used_bid_orders.empty:
            used_bid_orders['price'] = []
            used_bid_orders['volume'] = []
        else:
            used_bid_orders.columns = ['price', 'volume']

        prices['volume'] = volumes
        self._reset_parameter()
        return prices, used_orders['single_ask'], used_orders['linked_ask'],\
               used_orders['exclusive_ask'], used_bid_orders


if __name__ == "__main__":
    from systems.generation_powerPlant import PowerPlant, visualize_orderbook
    from systems.demand import HouseholdModel
    logging.basicConfig()

    max_p = 1500
    min_p = max_p*0.2
    plant = {'unitID': 'x',
             'fuel': 'coal',
             'maxPower': max_p,  # kW
             'minPower': min_p,  # kW
             'eta': 0.4,  # Wirkungsgrad
             'P0': 0,
             'chi': 0.407 / 1e3,  # t CO2/kWh
             'stopTime': 12,  # hours
             'runTime': 6,  # hours
             'gradP': min_p,  # kW/h
             'gradM': min_p,  # kW/h
             'on': 2,  # running since
             'off': 0,
             'startCost': 1e3  # €/Start
             }
    steps = np.array([-100, 0, 100])

    power_price = 50 * np.ones(48)
    power_price[18:24] = 0
    power_price[24:] = 50
    co = np.ones(48) * 23.8  # * np.random.uniform(0.95, 1.05, 48)     # -- Emission Price     [€/t]
    gas = np.ones(48) * 0.03  # * np.random.uniform(0.95, 1.05, 48)    # -- Gas Price          [€/kWh]
    lignite = np.ones(48) * 0.015  # * np.random.uniform(0.95, 1.05)   # -- Lignite Price      [€/kWh]
    coal = np.ones(48) * 0.02  # * np.random.uniform(0.95, 1.05)       # -- Hard Coal Price    [€/kWh]
    nuc = np.ones(48) * 0.01  # * np.random.uniform(0.95, 1.05)        # -- nuclear Price      [€/kWh]

    prices = dict(power=power_price, gas=gas, co=co, lignite=lignite, coal=coal, nuc=nuc)
    prices = pd.DataFrame(data=prices, index=pd.date_range(start='2018-01-01', freq='h', periods=48))

    pwp = PowerPlant(T=24, steps=steps, **plant)
    power = pwp.optimize(date='2018-01-01', weather=None,
                         prices=prices)

    o_book = pwp.get_orderbook()

    min_bid = -500/1e3
    o_book.loc[o_book['price'] < min_bid, 'price'] = min_bid
    # order book for first day without market
    # visualize_orderbook(o_book)

    house = HouseholdModel(24, 9e6)
    house.set_parameter(date='2018-01-01', weather=None,
                        prices=prices)
    house.optimize()
    demand = house.demand['power']

    def demand_order_book(demand, house):
        order_book = {}
        for t in house.t:
            if -demand[t] < 0:
                order_book[t] = dict(type='demand',
                                     hour=t,
                                     block_id=t,
                                     name='DEM',
                                     price=3000,  # €/kWh
                                     volume=-demand[t])

        demand_order = pd.DataFrame.from_dict(order_book, orient='index')
        demand_order = demand_order.set_index(['block_id', 'hour', 'name'])
        return demand_order

    demand[2:4] = 1
    demand_order = demand_order_book(demand, house)
    # plot demand which is matched by pwp
    #
    #### add renewables
    res_order = demand_order.copy()
    res_order = res_order.reset_index()
    res_order['name'] = 'RES'
    res_order = res_order.set_index(['block_id', 'hour', 'name'])
    res_order['volume'] = -0.2 * res_order['volume']
    res_order['price'] = -0.5
    res_order['type'] = 'generation'
    # ####
    #
    my_market = DayAheadMarket()

    hourly_bid = {}
    for key, value in demand_order.to_dict(orient='index').items():
        hourly_bid[key] = (value['price'], value['volume'])

    # hourly_ask = {}
    # for key, value in res_order.to_dict(orient='index').items():
    #     hourly_ask[key] = (value['price'], value['volume'])

    linked_orders = {}
    for key, value in o_book.to_dict(orient='index').items():
        linked_orders[key] = (value['price'], value['volume'], value['link'])

    # # print(linked_orders)
    my_market.set_parameter({}, hourly_bid, linked_orders, {})
    # optimize and unpack
    result = my_market.optimize()
    prices_market, used_ask_orders, used_linked_orders, used_exclusive_orders, used_bid_orders = result
    # my_market.model.use_linked_order.pprint()

    committed_power = used_linked_orders.groupby('hour').sum()['volume']
    # assert committed_power[23] == 300, 'pwp bids negative values'

    # plot committed power of the pwp for results of first day
    comm = np.zeros(24)
    comm[committed_power.index] = committed_power
    committed_power.plot()
    (-demand_order['volume']).plot()
    # (res_order['volume']).plot()

    # power = pwp.optimize_post_market(comm)
    ################ second day ##############
    #
    # import matplotlib.pyplot as plt
    #
    # plt.plot(power)
    # plt.show()
    #
    # power = pwp.optimize(date='2018-01-02', weather=None,
    #                      prices=prices)
    # o_book = pwp.get_orderbook()
    # visualize_orderbook(o_book)
    #
    # house.set_parameter(date='2018-01-02', weather=None,
    #                     prices=prices)
    # house.optimize()
    # demand = house.demand['power']
    #
    # demand_order = demand_order_book(demand, house)
    #
    # hourly_bid = {}
    # for key, value in demand_order.to_dict(orient='index').items():
    #     hourly_bid[key] = (value['price'], value['volume'])
    #
    # linked_orders = {}
    # for key, value in o_book.to_dict(orient='index').items():
    #     linked_orders[key] = (value['price'], value['volume'], value['link'])
    #
    # my_market.set_parameter({}, hourly_bid, linked_orders, {})
    # prices_market, used_ask_orders, used_linked_orders, used_exclusive_orders, used_bid_orders = my_market.optimize()
    #
    # committed_power = used_linked_orders.groupby('hour').sum()['volume']
    # comm = np.zeros(24)
    # comm[committed_power.index] = committed_power
    # committed_power.plot()
    # power = pwp.optimize_post_market(comm)
    # import matplotlib.pyplot as plt
    #
    # plt.plot(power)
    # plt.show()
    # my_market.model.use_linked_order.pprint()
