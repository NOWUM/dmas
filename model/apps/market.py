from gurobipy import *
import numpy as np


class market:

    def __init__(self):
        self.ask_orders = {}
        self.bid_orders = {}
        self.bid_check = {}

        self.m = Model('dayAheadMarket')
        self.m.Params.OutputFlag = 1
        self.m.Params.TimeLimit = 60
        self.m.Params.MIPGap = 0.05
        self.m.__len__ = 1

    def set_parameter(self, bid, ask):

        # check asks
        parts = np.unique([key[3] for key in ask.keys()])
        for part in parts:
            blocks = np.unique([key[0] for key in ask.keys() if part == key[3]])
            if len(blocks) > 0:
                check_block = np.arange(blocks[-1] + 1)
                diff = np.setdiff1d(check_block, blocks)
                if len(diff) == 0:
                    tmp = {}
                    for key, value in ask.items():
                        if key[3] == part:
                            tmp.update({key: value})
                    self.ask_orders.update(tmp)

        # check bids
        parts = np.unique([key[3] for key in bid.keys()])
        for part in parts:
            blocks = np.unique([key[0] for key in bid.keys() if part == key[3]])
            if len(blocks) > 0:
                check_block = np.arange(blocks[-1] + 1)
                diff = np.setdiff1d(check_block, blocks)
                if len(diff) == 0:
                    tmp_1 = {}
                    tmp_2 = {}
                    ask = {}
                    for key, value in bid.items():
                        if key[3] == part:
                            if value[0] == 3000:
                                tmp_1.update({key: value})
                            else:
                                x = (key[0], key[1], key[2], key[3] + '_bid')
                                ask.update({x: value})
                                tmp_1.update({key: (3000, value[1], value[2])})
                            tmp_2.update({key: value})
                    self.bid_orders.update(tmp_1)
                    self.bid_check.update(tmp_2)
                    self.ask_orders.update(ask)

    def optimize(self):
        # Step 0 initialize model and add magic power source with maximal price (prevent infeasible model)
        big_M = 5000
        max_prc = [np.round(5000, 2) for i in range(24)]
        max_vol = self.m.addVars(range(24), vtype=GRB.CONTINUOUS, name='magicSource_', lb=0.0, ub=GRB.INFINITY)

        constraint_counter = 0

        # Step 1 initialize orders
        # -----------------------------------------------------------------------------------------------------------
        # split in dictionary ids, id:prc, id:vol, id:linked
        ask_id, ask_prc, ask_vol, ask_block = multidict(self.ask_orders)
        # get all ask agents
        ask_agents = np.unique([a[-1] for a in ask_id])
        # get all blocks and dependency per ask agent
        ask_blocks = tuplelist([(i, agent, ask_block.select(i, '*', '*', str(agent))[0]) for agent in ask_agents
                                for i in range(ask_id.select('*', '*', '*', str(agent))[-1][0] + 1) if
                                ask_block.select(i, '*', '*', str(agent))[0] != 'x'])

        # split in dictionary ids, id:prc, id:vol, _
        bid_id, bid_prc, bid_vol, _ = multidict(self.bid_orders)
        # get all bid agents
        bid_agents = np.unique([b[-1] for b in bid_id])

        # Step 2 initialize binary variables for blocks and orders
        # -----------------------------------------------------------------------------------------------------------
        # used block to meet the demand
        used_ask_blocks = self.m.addVars(ask_blocks, vtype=GRB.BINARY, name='askBlock_')
        # used ask orders in block
        used_ask_orders = self.m.addVars(ask_id, vtype=GRB.BINARY, name='askOrder_')
        # used bid orders (no blocks considered)
        # used_bid_orders = self.m.addVars(bid_id, vtype=GRB.BINARY, name='bidOrder_')

        # Step 3 initialize cost variable for objective function (minimize sum(cost for each hour))
        # -----------------------------------------------------------------------------------------------------------
        # resulting costs for each hour
        x = self.m.addVars(range(24), vtype=GRB.CONTINUOUS, name='costs', lb=-GRB.INFINITY, ub=GRB.INFINITY)

        # Step 4 set constraint: If all orders for one agent in one block are used -> set block id = True
        # -----------------------------------------------------------------------------------------------------------
        for agent in ask_agents:
            self.m.addConstrs(used_ask_blocks[b] * len(used_ask_orders.select(b[0], '*', '*', str(agent))) ==
                              quicksum(used_ask_orders.select(b[0], '*', '*', str(agent)))
                              for b in ask_blocks.select('*', str(agent), '*'))
        self.m.update()
        constraint_counter = len(self.m.getConstrs())
        print('added %s constraints' % constraint_counter)

        # Step 5 set constraint: If parent block of an agent is used -> enable usage of child block
        # -----------------------------------------------------------------------------------------------------------
        for agent in ask_agents:
            self.m.addConstrs(used_ask_blocks[b] <= quicksum(used_ask_blocks.select(b[-1], str(agent), '*'))
                         for b in ask_blocks.select('*', str(agent), '*'))
        self.m.update()
        print('added %s constraints' % (len(self.m.getConstrs()) - constraint_counter))
        constraint_counter = len(self.m.getConstrs())

        # Step 6 set constraint: Enable only bids with price >= ask price for each hour
        # -----------------------------------------------------------------------------------------------------------
        # for agent in bid_agents:
        #     for i in range(24):
        #         for b in bid_id.select('*', i, '*', str(agent)):
        #             self.m.addConstr(
        #                 big_M * used_bid_orders[b] >= bid_prc[b] - max(ask_prc.select('*', i, '*', '*') + [-501]))
        #             self.m.addConstr(
        #                 big_M * (1 - used_bid_orders[b]) >= max(ask_prc.select('*', i, '*', '*') + [-501]) - bid_prc[b])
        # self.m.update()
        # print('added %s constraints' % (len(self.m.getConstrs()) - constraint_counter))
        # constraint_counter = len(self.m.getConstrs())

        # Step 7 set constraint: Meet Demand in each hour
        # -----------------------------------------------------------------------------------------------------------
        self.m.addConstrs(quicksum(ask_vol[o] * used_ask_orders[o] for o in ask_id.select('*', i, '*', '*'))
                          + max_vol[i] == quicksum(bid_vol[o] for o in bid_id.select('*', i, '*', '*'))
                          for i in range(24))

        self.m.update()
        print('added %s constraints' % (len(self.m.getConstrs()) - constraint_counter))
        constraint_counter = len(self.m.getConstrs())

        # Step 8 set constraint: Cost for each hour
        # -----------------------------------------------------------------------------------------------------------
        self.m.addConstrs(x[i] == quicksum(ask_vol[o] * ask_prc[o] * used_ask_orders[o]
                                      for o in ask_id.select('*', i, '*', '*')) + max_vol[i] * max_prc[i]
                     for i in range(24))

        self.m.update()
        print('added %s constraints' % (len(self.m.getConstrs()) - constraint_counter))
        constraint_counter = len(self.m.getConstrs())

        self.m.setObjective(quicksum(x[i] for i in range(24)), GRB.MINIMIZE)

        self.m.update()
        self.m.optimize()

        # Step 9 get MCP for each hour (max price from last order that is used)
        # -----------------------------------------------------------------------------------------------------------
        mcp = [-500 for _ in range(24)]
        for id_ in ask_id:
            if used_ask_orders[id_].x == 1 and '_bid' not in id_:
                if ask_prc[id_] >= mcp[id_[1]]:
                    mcp[id_[1]] = ask_prc[id_]

        # Step 10 get volumes for each hour per ask agent
        # -----------------------------------------------------------------------------------------------------------
        ask_volumes = []
        for i in range(24):
            volumes = {}
            for agent in ask_agents:
                vol = 0
                if '_bid' not in agent:
                    for order in ask_id.select('*', i, '*', str(agent)):
                        if used_ask_orders[order].x == 1:
                            vol += ask_vol[order]
                    volumes.update({agent: np.round(vol, 2)})
            ask_volumes.append(dict(volume=volumes))

        # Step 11 get volumes for each hour per bid agent
        # -----------------------------------------------------------------------------------------------------------
        bid_volumes = []
        total_volumes = []
        bid_id, bid_prc, bid_vol, _ = multidict(self.bid_check)
        for i in range(24):
            volumes = {}
            total_volume = 0
            for agent in bid_agents:
                vol = 0
                for order in bid_id.select('*', i, '*', str(agent)):
                    if bid_prc[order] > mcp[i]:
                        vol += bid_vol[order]
                volumes.update({agent: np.round(vol, 2)})
                total_volume += vol
            total_volumes.append(total_volume)
            bid_volumes.append(dict(volume=volumes))

        result = [(ask_volumes[i], bid_volumes[i], mcp[i], total_volumes[i]) for i in range(24)]

        #self.ask_orders = {}
        #self.bid_orders = {}
        #self.bid_check = {}

        return result

def build_test_ask(name):
    ask_order = {}
    for id_num in [0, 1, 2]:
        linked = 0
        start = np.random.randint(low=0, high=8)
        end = np.random.randint(low=8, high=24)
        for i in range(start, end):
            volumes = np.random.uniform(0, 10, 1)
            prices = np.random.uniform(1, 5, 1)
            for j in range(len(volumes)):
                ask_order.update({(id_num, i, j, name): (np.round(prices[j], 2),
                                                         np.round(volumes[j], 2),
                                                         int(linked))})
    return ask_order


def build_test_bid(name):
    bid_order = {}
    for id_num in [0]:
        linked = id_num
        for i in range(24):
            volumes = np.random.uniform(5, 20, 1)
            prices = np.random.uniform(100, 150, 1)
            for j in range(len(volumes)):
                bid_order.update({(id_num, i, j, name): (np.round(prices[j], 2),
                                                         np.round(volumes[j], 2),
                                                         int(linked))})
    return bid_order


if __name__ == "__main__":

    my_market = market()

