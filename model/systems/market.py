import numpy as np
import pandas as pd
from pyomo.environ import Var, Objective, SolverFactory, ConcreteModel, Reals, Binary, \
    minimize, maximize, quicksum, ConstraintList
from pyomo.environ import value as get_real_number
import logging


class DayAheadMarket:

    def __init__(self):

        self.logger = logging.getLogger('market')
        self.hourly_ask_total = {}
        self.hourly_ask_orders = {}

        self.hourly_bid_total = {}
        self.hourly_bid_orders = {}

        self.linked_total = {}
        self.hourly_linked_orders = {}

        self.exclusive_total = {}
        self.hourly_exclusive_orders = {}

        self.parent_blocks = {}

        self.model = ConcreteModel()
        self.opt = SolverFactory('gurobi', solver_io='python')

        self.t = np.arange(24)

    def set_parameter(self, hourly_ask, hourly_bid, linked_orders, exclusive_orders):

        self.hourly_ask_total.update(hourly_ask)
        self.hourly_bid_total.update(hourly_bid)
        self.linked_total.update(linked_orders)
        self.exclusive_total.update(exclusive_orders)

        self.hourly_ask_orders = {t: [] for t in self.t}
        self.hourly_bid_orders = {t: [] for t in self.t}

        self.hourly_linked_orders = {t: [] for t in self.t}
        self.hourly_exclusive_orders = {t: [] for t in self.t}

        for t in self.t:
            # hourly orders
            for key in self.hourly_ask_total.keys():
                block, hour, order, name = key
                if hour == t:
                    self.hourly_ask_orders[t].append((block, order, name))
            for key in self.hourly_bid_total.keys():
                block, hour, order, name = key
                if hour == t:
                    self.hourly_bid_orders[t].append((block, order, name))
            # linked orders
            for key in self.linked_total.keys():
                block, hour, order, name = key
                if hour == t:
                    self.hourly_linked_orders[t].append((block, order, name))
            # exclusive orders
            for key in self.exclusive_total.keys():
                block, hour, order, name = key
                if hour == t:
                    self.hourly_exclusive_orders[t].append((block, order, name))

        self.parent_blocks = {}
        for key, value in self.linked_total.items():
            block, _, _, agent = key
            _, _, parent_id = value
            child_key = (block, agent)
            self.parent_blocks[child_key] = parent_id

    def reset_parameter(self):
        self.hourly_ask_total = {}
        self.hourly_ask_orders = {}

        self.hourly_bid_total = {}
        self.hourly_bid_orders = {}

        self.linked_total = {}
        self.hourly_linked_orders = {}

        self.exclusive_total = {}
        self.hourly_exclusive_orders = {}

        self.parent_blocks = {}

    def get_unique(self, x):
        return list(dict.fromkeys(x))

    def optimize(self):

        self.model.clear()

        # Step 1 initialize binary variables for hourly ask block per agent and id
        self.model.use_hourly_ask = Var(self.get_unique((block, hour, order_id, agent) for block, hour, order_id, agent
                                                        in self.hourly_ask_total.keys()), within=Binary)

        # Step 3 initialize binary variables for ask order in block per agent
        self.model.use_linked_order = Var(self.get_unique([(block, hour, order, agent) for block, hour, order, agent
                                                           in self.linked_total.keys()]), within=Binary)

        # Step 4 initialize binary variables for exclusive block and agent
        self.model.use_exclusive_block = Var(self.get_unique([(block, agent) for block, _, agent
                                                              in self.exclusive_total.keys()]), within=Binary)

        # Step 6 set constraint: If parent block of an agent is used -> enable usage of child block
        self.model.enable_child_block = ConstraintList()
        self.model.mother_bid = ConstraintList()
        for data in self.get_unique([(block, agent) for block, _, _, agent in self.linked_total.keys()]):
            block, agent = data
            parent_id = self.parent_blocks[block, agent]
            if parent_id != -1:
                self.model.enable_child_block.add(quicksum(self.model.use_linked_order[block, :, :, agent]) <=
                                                  2 * quicksum(self.model.use_linked_order[parent_id, :, :, agent]))

            if block ==0:
                # mother bid must exist with at least one entry
                # either the whole mother bid can be used or none
                mother_bid_counter = len(list(self.model.use_linked_order[0, :, 0, agent]))
                first_mother_hour = list(self.model.use_linked_order[0, :, 0, agent])[0]
                self.model.mother_bid.add(quicksum(self.model.use_linked_order[0, :, 0, agent]) == mother_bid_counter * first_mother_hour)

        # Constraints for exclusive block orders
        # ------------------------------------------------

        # Step 7 set constraint: only one scheduling can be used
        self.model.one_exclusive_block = ConstraintList()
        for data in self.get_unique([(agent,) for _, _, _, agent in self.exclusive_total.keys()]):
            agent = data
            self.model.one_exclusive_block.add(1 >= quicksum(self.model.use_exclusive_block[:, agent]))

        magic_source = [-1 * (quicksum(self.linked_total[block, t, order, name][1] *
                                       self.model.use_linked_order[block, t, order, name]
                                       for block, order, name in self.hourly_linked_orders[t])
                              + quicksum(self.exclusive_total[block, t, name][1] *
                                         self.model.use_linked_order[block, name]
                                         for block, name in self.hourly_exclusive_orders[t])
                              + quicksum(self.hourly_ask_total[block, t, order, name][1] *
                                         self.model.use_hourly_ask[block, t, order, name]
                                         for block, order, name in self.hourly_ask_orders[t])
                              + quicksum(self.hourly_bid_total[block, t, order, name][1]
                                         for block, order, name in self.hourly_bid_orders[t])) for t in self.t]

        # generation must be smaller than demand
        self.model.gen_dem = ConstraintList()
        for t in self.t:
            self.model.gen_dem.add(quicksum(self.linked_total[block, t, order, name][1] *
                                            self.model.use_linked_order[block, t, order, name]
                                            for block, order, name in self.hourly_linked_orders[t])
                                   + quicksum(self.exclusive_total[block, t, name][1] *
                                              self.model.use_linked_order[block, name]
                                              for block, name in self.hourly_exclusive_orders[t])
                                   + quicksum(self.hourly_ask_total[block, t, order, name][1] *
                                              self.model.use_hourly_ask[block, t, order, name]
                                              for block, order, name in self.hourly_ask_orders[t])
                                   <= -1 * quicksum(self.hourly_bid_total[block, t, order, name][1]
                                                    for block, order, name in self.hourly_bid_orders[t]))

        # Step 9 set constraint: Cost for each hour
        generation_cost = quicksum(quicksum(self.linked_total[block, t, order, name][0] *
                                            self.model.use_linked_order[block, t, order, name]
                                            for block, order, name in self.hourly_linked_orders[t])
                                   + quicksum(self.exclusive_total[block, t, name][0] *
                                              self.model.use_exclusive_block[block, name]
                                              for block, name in self.hourly_exclusive_orders[t])
                                   + quicksum(self.hourly_ask_total[block, t, order, name][0] *
                                              self.model.use_hourly_ask[block, t, order, name]
                                              for block, order, name in self.hourly_ask_orders[t])
                                   + magic_source[t] * 1e9 for t in self.t)

        self.model.obj = Objective(expr=generation_cost, sense=minimize)

        try:
            result = self.opt.solve(self.model, options={'MIPGap': 0.1, 'TimeLimit': 60})
            print(result)
        except Exception:
            self.logger.exception('error solving optimization problem')
            self.logger.error(f'Model: {self.model}')

        prices = []
        for t in self.t:
            orders = []
            for block, order, name in self.hourly_ask_orders[t]:
                price = self.model.use_hourly_ask[block, t, order, name].value \
                        * self.hourly_ask_total[block, t, order, name][0]
                orders.append(price)
            for block, order, name in self.hourly_linked_orders[t]:
                price = self.model.use_linked_order[block, t, order, name].value \
                        * self.linked_total[block, t, order, name][0]
                orders.append(price)
            for block, order, name in self.hourly_exclusive_orders[t]:
                price = self.model.use_exclusive_block[block, t, order, name].value \
                        * self.exclusive_total[block, t, order, name][0]
                orders.append(price)

            if not orders:
                raise Exception('No Orders available - Is any demand connected?')
            prices.append(max(orders))
        prices = pd.DataFrame(data=dict(price=prices))

        used_ask_orders = {}
        for t in self.t:
            for block, order, name in self.hourly_ask_orders[t]:
                if self.model.use_hourly_ask[block, t, order, name].value:
                    used_ask_orders.update({(block, t, order, name): self.hourly_ask_total[block, t, order, name]})
        used_ask_orders = pd.DataFrame.from_dict(used_ask_orders, orient='index')
        used_ask_orders.index = pd.MultiIndex.from_tuples(used_ask_orders.index,
                                                          names=['block_id', 'hour', 'order_id', 'name'])
        if used_ask_orders.empty:
            used_ask_orders['price'] = []
            used_ask_orders['volume'] = []
        else:
            used_ask_orders.columns = ['price', 'volume']

        used_linked_orders = {}
        for t in self.t:
            for block, order, name in self.hourly_linked_orders[t]:
                if self.model.use_linked_order[block, t, order, name].value:
                    used_linked_orders.update({(block, t, order, name): self.linked_total[block, t, order, name]})
        used_linked_orders = pd.DataFrame.from_dict(used_linked_orders, orient='index')
        used_linked_orders.index = pd.MultiIndex.from_tuples(used_linked_orders.index,
                                                             names=['block_id', 'hour', 'order_id', 'name'])
        if used_linked_orders.empty:
            used_linked_orders['price'] = []
            used_linked_orders['volume'] = []
            used_linked_orders['link'] = []
        else:
            used_linked_orders.columns = ['price', 'volume', 'link']

        used_exclusive_orders = {}
        for t in self.t:
            for block, order, name in self.hourly_exclusive_orders[t]:
                if self.model.use_linked_order[block, t, order, name].value:
                    used_exclusive_orders.update({(block, t, order, name): self.exclusive_total[block, t, order, name]})
        used_exclusive_orders = pd.DataFrame.from_dict(used_exclusive_orders, orient='index')
        used_exclusive_orders.index = pd.MultiIndex.from_tuples(used_exclusive_orders.index,
                                                                names=['block_id', 'hour', 'order_id', 'name'])
        if used_exclusive_orders.empty:
            used_exclusive_orders['price'] = []
            used_exclusive_orders['volume'] = []
        else:
            used_exclusive_orders.columns = ['price', 'volume']

        used_bid_orders = self.hourly_bid_total
        volumes = []
        sum_magic_source = 0
        for t in self.t:
            sum_magic_source += get_real_number(magic_source[t])
            volume = 0
            for block, order, name in self.hourly_bid_orders[t]:
                volume += (-1) * self.hourly_bid_total[block, t, order, name][1]
            volumes.append(volume)
        print(sum_magic_source)
        self.logger.info(f'Got {sum_magic_source:.2f} kWh from Magic source')

        used_bid_orders = pd.DataFrame.from_dict(used_bid_orders, orient='index')
        used_bid_orders.index = pd.MultiIndex.from_tuples(used_bid_orders.index,
                                                          names=['block_id', 'hour', 'order_id', 'name'])
        if used_bid_orders.empty:
            used_bid_orders['price'] = []
            used_bid_orders['volume'] = []
        else:
            used_bid_orders.columns = ['price', 'volume']

        prices['volume'] = volumes
        self.reset_parameter()
        return prices, used_ask_orders, used_linked_orders, used_exclusive_orders, used_bid_orders


if __name__ == "__main__":
    from systems.generation_powerPlant import PowerPlant, visualize_orderbook
    from systems.demand import HouseholdModel

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

    pwp = PowerPlant(T=24, steps=steps, **plant)
    pwp.set_parameter(date='2018-01-01', weather=None,
                      prices=prices)
    power = pwp.optimize()
    o_book = pwp.get_orderbook()
    # order book for first day without market
    visualize_orderbook(o_book)

    house = HouseholdModel(24, 3e6)
    house.set_parameter(date='2018-01-01', weather=None,
                        prices=prices)
    house.build_model()
    house.optimize()
    demand = house.demand['power']


    def demand_order_book(demand, house):
        order_book = {}
        for t in house.t:
            if -demand[t] < 0:
                order_book[t] = dict(type='demand',
                                     hour=t,
                                     order_id=0,
                                     block_id=t,
                                     name='DEM',
                                     price=3000,  # €/kWh
                                     volume=-demand[t])

        demand_order = pd.DataFrame.from_dict(order_book, orient='index')
        demand_order = demand_order.set_index(['block_id', 'hour', 'order_id', 'name'])
        return demand_order

    #demand[2:4] = 1
    demand_order = demand_order_book(demand, house)
    # plot demand which is matched by pwp

    #### add renewables
    res_order = demand_order.copy()
    res_order = res_order.reset_index() 
    res_order['name'] = 'RES'
    res_order = res_order.set_index(['block_id', 'hour', 'order_id', 'name'])
    res_order['volume'] = -0.2 * res_order['volume']
    res_order['price'] = -0.5
    res_order['type'] = 'generation'
    ####

    my_market = DayAheadMarket()

    hourly_bid = {}
    for key, value in demand_order.to_dict(orient='index').items():
        hourly_bid[key] = (value['price'], value['volume'])

    hourly_ask = {}
    for key, value in res_order.to_dict(orient='index').items():
        hourly_ask[key] = (value['price'], value['volume'])

    linked_orders = {}
    for key, value in o_book.to_dict(orient='index').items():
        linked_orders[key] = (value['price'], value['volume'], value['link'])

    my_market.set_parameter(hourly_ask, hourly_bid, linked_orders, {})
    # optimize and unpack
    result = my_market.optimize()
    prices_market, used_ask_orders, used_linked_orders, used_exclusive_orders, used_bid_orders = result
    my_market.model.use_linked_order.pprint()

    committed_power = used_linked_orders.groupby('hour').sum()['volume']


    # plot committed power of the pwp for results of first day
    comm = np.zeros(24)
    comm[committed_power.index] = committed_power
    committed_power.plot()
    (-demand_order['volume']).plot()
    (res_order['volume']).plot()

    ################# second day ##############
    pwp.committed_power = comm
    power = pwp.optimize()
    import matplotlib.pyplot as plt

    plt.plot(power)
    plt.show()

    pwp.set_parameter(date='2018-01-02', weather=None,
                      prices=prices)
    power = pwp.optimize()
    o_book = pwp.get_orderbook()
    visualize_orderbook(o_book)

    house.set_parameter(date='2018-01-02', weather=None,
                        prices=prices)
    house.build_model()
    house.optimize()
    demand = house.demand['power']

    demand_order = demand_order_book(demand, house)

    hourly_bid = {}
    for key, value in demand_order.to_dict(orient='index').items():
        hourly_bid[key] = (value['price'], value['volume'])

    linked_orders = {}
    for key, value in o_book.to_dict(orient='index').items():
        linked_orders[key] = (value['price'], value['volume'], value['link'])

    my_market.set_parameter({}, hourly_bid, linked_orders, {})
    prices_market, used_ask_orders, used_linked_orders, used_exclusive_orders, used_bid_orders = my_market.optimize()

    committed_power = used_linked_orders.groupby('hour').sum()['volume']
    comm = np.zeros(24)
    comm[committed_power.index] = committed_power
    committed_power.plot()
    pwp.committed_power = comm
    power = pwp.optimize()
    import matplotlib.pyplot as plt

    plt.plot(power)
    plt.show()
    my_market.model.use_linked_order.pprint()

