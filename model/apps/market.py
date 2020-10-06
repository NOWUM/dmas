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
        id_ = np.unique([key[3] for key in ask.keys()])                             # unique id for each order
        for i in id_:
            blocks = np.unique([key[0] for key in ask.keys() if i == key[3]])       # blocks for each unique id
            if len(blocks) > 0:                                                     # check if any block exists
                check_block = np.arange(blocks[-1] + 1)                             # array to proof consistency
                diff = np.setdiff1d(check_block, blocks)                            # check if block number
                if len(diff) == 0:                                                  # increments correctly
                    self.ask_orders.update({key: value for key, value in ask.items()
                                            if key[3] == i})                        # add orders clearing

        # check bids
        id_ = np.unique([key[3] for key in bid.keys()])                             # unique id for each order
        for i in id_:
            blocks = np.unique([key[0] for key in bid.keys() if i == key[3]])       # blocks for each unique id
            if len(blocks) > 0:                                                     # check if any block exists
                check_block = np.arange(blocks[-1] + 1)                             # array to proof consistency
                diff = np.setdiff1d(check_block, blocks)                            # check if block number
                if len(diff) == 0:                                                  # increments correctly
                    self.bid_check.update({key: value for key, value in bid.items() # add orders to bid dict to
                                           if key[3] == i})                         # get right orders
                    # split in ask in bid for elastic demand
                    # --> add inelastic demand
                    self.bid_orders.update({key: value for key, value in bid.items()
                                            if value[0] == 3000 and key[3] == i})
                    # --> make elastic demand to inelastic demand
                    self.bid_orders.update({key: (3000, value[1], value[2]) for key, value in bid.items()
                                            if value[0] != 3000 and key[3] == i})
                    # --> add ask orders for elastic demand
                    self.ask_orders.update({(key[0], key[1], key[2], key[3] + '_b'): value for key, value in bid.items()
                                            if value[0] != 3000 and key[3] == i})

    def optimize(self):
        # Step 0 initialize model and add magic power source with maximal price (prevent infeasible model)
        max_prc = [np.round(50000, 2) for i in range(24)]
        max_vol = self.m.addVars(range(24), vtype=GRB.CONTINUOUS, name='magicSource_', lb=0.0, ub=GRB.INFINITY)

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

        # Step 6 set constraint: Meet Demand in each hour
        # -----------------------------------------------------------------------------------------------------------
        self.m.addConstrs(quicksum(ask_vol[o] * used_ask_orders[o] for o in ask_id.select('*', i, '*', '*'))
                          + max_vol[i] == quicksum(bid_vol[o] for o in bid_id.select('*', i, '*', '*'))
                          for i in range(24))

        self.m.update()
        print('added %s constraints' % (len(self.m.getConstrs()) - constraint_counter))
        constraint_counter = len(self.m.getConstrs())

        # Step 7 set constraint: Cost for each hour
        # -----------------------------------------------------------------------------------------------------------
        self.m.addConstrs(x[i] == quicksum(ask_vol[o] * ask_prc[o] * used_ask_orders[o]
                                           for o in ask_id.select('*', i, '*', '*')) + max_vol[i] * max_prc[i]
                                  for i in range(24))

        self.m.update()
        print('added %s constraints' % (len(self.m.getConstrs()) - constraint_counter))

        self.m.setObjective(quicksum(x[i] for i in range(24)), GRB.MINIMIZE)

        self.m.update()
        self.m.optimize()

        # Step 8 get MCP for each hour (max price from last order that is used)
        # -----------------------------------------------------------------------------------------------------------
        mcp = [max([ask_prc[id_] * used_ask_orders[id_].x for id_ in ask_id.select('*', i, '*', '*')
                    if used_ask_orders[id_].x == 1])
               for i in range(24)]

        # Step 9 get volumes for each hour per ask agent
        # -----------------------------------------------------------------------------------------------------------
        ask_volumes = []
        for i in range(24):
            volumes = {}
            for agent in ask_agents:
                vol = sum([ask_vol[id_] * used_ask_orders[id_].x for id_ in ask_id.select('*', i, '*', str(agent))
                           if ask_prc[id_] <= mcp[i]])
                if '_b' not in agent:
                    volumes.update({agent: np.round(vol, 2)})
            ask_volumes.append(dict(volume=volumes))

        # Step 10 get volumes for each hour per bid agent
        # -----------------------------------------------------------------------------------------------------------
        bid_volumes = []
        total_volumes = []
        bid_id, bid_prc, bid_vol, _ = multidict(self.bid_check)
        for i in range(24):
            volumes = {}
            sum_ = 0
            for agent in bid_agents:
                # --> adjust volume  if magic source is last ask in merit order
                if max_vol[i].x <= 0.02:
                    vol = sum([bid_vol[id_] for id_ in bid_id.select('*', i, '*', str(agent))
                               if bid_prc[id_] > mcp[i]])
                else:
                    vol = sum([bid_vol[id_] for id_ in bid_id.select('*', i, '*', str(agent))])
                sum_ += vol
                volumes.update({agent: np.round(vol, 2)})
            bid_volumes.append(dict(volume=volumes))
            total_volumes.append(sum_)

        result = [(ask_volumes[i], bid_volumes[i], mcp[i], total_volumes[i], max_vol[i].x) for i in range(24)]

        return result


if __name__ == "__main__":

    my_market = market()

