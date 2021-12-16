import numpy as np
import pandas as pd
from pyomo.environ import  Var, Objective, SolverFactory, ConcreteModel, Reals, Binary, \
    minimize, quicksum, ConstraintList


class DayAheadMarket:

    def __init__(self):
        self.ask_orders_total = {}
        self.bid_orders_total = {}
        self.ask_orders_per_hour = {}
        self.bid_orders_per_hour = {}
        self.parent_blocks = {}

        self.model = ConcreteModel()
        self.opt = SolverFactory('gurobi')

        self.t = np.arange(24)

    def set_parameter(self, bid, ask):

        self.ask_orders_total.update(ask)
        self.bid_orders_total.update(bid)

        self.ask_orders_per_hour = {t: [] for t in self.t}
        self.bid_orders_per_hour = {t: [] for t in self.t}
        for t in self.t:
            for key in self.ask_orders_total.keys():
                block, hour, order, name = key
                if hour == t:
                    self.ask_orders_per_hour[t].append((block, order, name))
            for key in self.bid_orders_total.keys():
                block, hour, order, name = key
                if hour == t:
                    self.bid_orders_per_hour[t].append((block, order, name))

        self.parent_blocks = {}
        for key, value in self.ask_orders_total.items():
            block, _, _, agent = key
            _, _, parent_id = value
            child_key = (block, agent)
            self.parent_blocks[child_key] = parent_id

    def reset_parameter(self):
        self.ask_orders_total = {}
        self.bid_orders_total = {}

        self.ask_orders_per_hour = {}
        self.bid_orders_per_hour = {}

        self.parent_blocks = {}

    def get_unique(self, x):
        return list(dict.fromkeys(x))

    def optimize(self):

        self.model.clear()

        # Step 1 initialize binary variables for ask block per agent
        self.model.use_ask_block = Var(self.get_unique([(block, agent) for block, _, _, agent in self.ask_orders_total.keys()]),
                                       within=Binary)
        # Step 2 initialize binary variables for ask order in block per agent
        self.model.use_ask_order = Var(self.get_unique([(block, hour, order, agent) for block, hour, order, agent
                                                  in self.ask_orders_total.keys()]), within=Binary)

        # Step 3 set constraint: If all orders for one agent in one block are used -> set block id = True
        self.model.all_orders_in_block = ConstraintList()
        for data in self.get_unique([(block, agent) for block, _, _, agent in self.ask_orders_total.keys()]):
            order_counter = 0
            block, agent = data
            for b, _, _, a in self.ask_orders_total.keys():
                if b == block and a == agent:
                    order_counter += 1
            self.model.all_orders_in_block.add(self.model.use_ask_block[block, agent] * order_counter
                                               == quicksum(self.model.use_ask_order[block, :, :, agent]))

        # Step 4 set constraint: If parent block of an agent is used -> enable usage of child block
        self.model.enable_child_block = ConstraintList()
        for data in self.get_unique([(block, agent) for block, _, _, agent in self.ask_orders_total.keys()]):
            block, agent = data
            parent_id = self.parent_blocks[block, agent]
            if parent_id != -1:
                self.model.enable_child_block.add(self.model.use_ask_block[block, agent]
                                                  <= self.model.use_ask_block[parent_id, agent])

        # # Step 4 set constraint: Meet Demand in each hour
        max_prc = [np.round(20000, 2) for i in range(24)]
        self.model.magic_source = Var(self.t, bounds=(0, None), within=Reals)

        self.model.demand = ConstraintList()
        for t in self.t:
            self.model.demand.add(quicksum(self.ask_orders_total[block, t, order, name][1] *
                                           self.model.use_ask_order[block, t, order, name]
                                           for block, order, name in self.ask_orders_per_hour[t])
                                  + self.model.magic_source[t]
                                  == sum(-1 * self.bid_orders_total[block, t, order, name][1]
                                         for block, order, name in self.bid_orders_per_hour[t]))

        # Step 5 set constraint: Cost for each hour
        self.model.generation_cost = Var(self.t, within=Reals, bounds=(0, None))
        self.model.costs = ConstraintList()
        for t in self.t:
            self.model.costs.add(quicksum(self.ask_orders_total[block, t, order, name][0] *
                                          self.model.use_ask_order[block, t, order, name]
                                          for block, order, name in self.ask_orders_per_hour[t])
                                 + self.model.magic_source[t] * max_prc[t] == self.model.generation_cost[t])

        self.model.obj = Objective(expr=quicksum(self.model.generation_cost[t] for t in self.t), sense=minimize)

        self.opt.solve(self.model)

        prices = [max([self.ask_orders_total[block, t, order, name][0] * self.model.use_ask_order[block, t, order, name].value
                       for block, order, name in self.ask_orders_per_hour[t]]) for t in self.t]
        prices = pd.DataFrame(data=dict(price=prices))
        #print(prices)

        used_ask_orders = {}
        for t in self.t:
            for block, order, name in self.ask_orders_per_hour[t]:
                if self.model.use_ask_order[block, t, order, name].value and '_b' not in name:
                    used_ask_orders.update({(block, t, order, name): self.ask_orders_total[block, t, order, name]})

        used_ask_orders = pd.DataFrame.from_dict(used_ask_orders, orient='index', columns=['price', 'volume', 'link'])
        used_ask_orders['type'] = 'generation'
        #print(used_ask_orders)

        used_bid_orders = {}
        volumes = []
        for t in self.t:
            volume = 0
            for block, order, name in self.bid_orders_per_hour[t]:
                volume += -1*self.bid_orders_total[block, t, order, name][1]
                used_bid_orders.update({(block, t, order, name): self.bid_orders_total[block, t, order, name]})
            for block, order, name in self.ask_orders_per_hour[t]:
                if self.model.use_ask_order[block, t, order, name].value and '_b' in name:
                    volume += -1 * self.ask_orders_total[block, t, order, name][1]
                    used_bid_orders.update({(block, t, order, name.replace('_b', '')): self.ask_orders_total[block, t, order, name]})
            volumes.append(volume)
        volumes = pd.DataFrame(data=dict(volume=volumes))
        used_bid_orders = pd.DataFrame.from_dict(used_bid_orders, orient='index', columns=['price', 'volume', 'link'])
        used_bid_orders['type'] = 'demand'
        #print(used_bid_orders)

        orders = pd.concat([used_ask_orders, used_bid_orders])
        orders.index = pd.MultiIndex.from_tuples(orders.index, names=['block_id', 'hour', 'order_id', 'name'])
        result = pd.concat([prices, volumes], axis=1)

        self.reset_parameter()

        return result, orders

if __name__ == "__main__":

    my_market = DayAheadMarket()