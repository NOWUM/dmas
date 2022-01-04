import numpy as np
import pandas as pd
from pyomo.environ import  Var, Objective, SolverFactory, ConcreteModel, Reals, Binary, \
    minimize, maximize, quicksum, ConstraintList


class DayAheadMarket:

    def __init__(self):


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
        # Step 2 initialize binary variables for linked ask block per agent
        self.model.use_linked_block = Var(self.get_unique([(block, agent) for block, _, _, agent
                                                            in self.linked_total.keys()]), within=Binary)

        # Step 3 initialize binary variables for ask order in block per agent
        self.model.use_linked_order = Var(self.get_unique([(block, hour, order, agent) for block, hour, order, agent
                                                            in self.linked_total.keys()]), within=Binary)

        # Step 4 initialize binary variables for exclusive block and agent
        self.model.use_exclusive_block = Var(self.get_unique([(block, agent) for block, _, agent
                                                              in self.exclusive_total.keys()]), within=Binary)

        # Constraints for linked block orders
        # ------------------------------------------------
        # Step 5 set constraint: If all orders for one agent in one block are used -> set block id = True
        self.model.all_orders_in_block = ConstraintList()
        for data in self.get_unique([(block, agent) for block, _, _, agent in self.linked_total.keys()]):
            order_counter = 0
            block, agent = data
            for b, _, _, a in self.linked_total.keys():
                if b == block and a == agent:
                    order_counter += 1
            self.model.all_orders_in_block.add(self.model.use_linked_block[block, agent] * order_counter
                                               >= quicksum(self.model.use_linked_order[block, :, :, agent]))

        # Step 6 set constraint: If parent block of an agent is used -> enable usage of child block
        self.model.enable_child_block = ConstraintList()
        for data in self.get_unique([(block, agent) for block, _, _, agent in self.linked_total.keys()]):
            block, agent = data
            parent_id = self.parent_blocks[block, agent]
            if parent_id != -1:
                self.model.enable_child_block.add(self.model.use_linked_block[block, agent]
                                                  <= self.model.use_linked_block[parent_id, agent])


        # Constraints for excluive block orders
        # ------------------------------------------------
        # Step 7 set constraint: only one scheduling can be used
        self.model.one_exclusive_block = ConstraintList()
        for data in self.get_unique([(agent,) for _, _, _, agent in self.exclusive_total.keys()]):
            agent = data
            self.model.one_exclusive_block.add(1 >= quicksum(self.model.use_exclusive_block[:, agent]))


        # # Step 8 set constraint: Meet Demand in each hour
        max_prc = [np.round(20000, 2) for i in range(24)]
        self.model.magic_source = Var(self.t, bounds=(0, None), within=Reals)

        self.model.demand = ConstraintList()
        for t in self.t:
            self.model.demand.add(quicksum(self.linked_total[block, t, order, name][1] *
                                           self.model.use_linked_order[block, t, order, name]
                                           for block, order, name in self.hourly_linked_orders[t])
                                  + quicksum(self.exclusive_total[block, t, name][1] *
                                             self.model.use_linked_order[block, name]
                                             for block, name in self.hourly_exclusive_orders[t])
                                  + quicksum(self.hourly_ask_total[block,t, order, name][1] *
                                             self.model.use_hourly_ask[block, t, order, name]
                                             for block, order, name in self.hourly_ask_orders[t])
                                  + self.model.magic_source[t]
                                  == sum(-1 * self.hourly_bid_total[block, t, order, name][1]
                                         for block, order, name in self.hourly_bid_orders[t]))

        # Step 5 set constraint: Cost for each hour
        self.model.generation_cost = Var(self.t, within=Reals)
        self.model.costs = ConstraintList()
        for t in self.t:
            self.model.costs.add(quicksum(self.linked_total[block, t, order, name][0] *
                                          self.model.use_linked_order[block, t, order, name]
                                          for block, order, name in self.hourly_linked_orders[t])
                                 + quicksum(self.exclusive_total[block, t, name][0] *
                                            self.model.use_exclusive_block[block, name]
                                            for block, name in self.hourly_exclusive_orders[t])
                                 + quicksum(self.hourly_ask_total[block, t, order, name][0] *
                                            self.model.use_hourly_ask[block, t, order, name]
                                            for block, order, name in self.hourly_ask_orders[t])
                                 + self.model.magic_source[t] * max_prc[t] == self.model.generation_cost[t])

        self.model.obj = Objective(expr=quicksum(self.model.generation_cost[t] for t in self.t), sense=minimize)

        result = self.opt.solve(self.model)
        print(result)

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
        for t in self.t:
            volume = 0
            for block, order, name in self.hourly_bid_orders[t]:
                volume += (-1) * self.hourly_bid_total[block, t, order, name][1]
            volumes.append(volume)
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

    my_market = DayAheadMarket()