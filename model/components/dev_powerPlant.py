# third party modules
import os
import gurobipy as gby
import numpy as np

# model modules
from components.energy_system import EnergySystem
os.chdir(os.path.dirname(os.path.dirname(__file__)))


def get_prices():
    power_price = np.concatenate((np.random.uniform(low=15, high=30, size=24),
                                  np.random.uniform(low=30, high=50, size=24)))
    co = np.ones(48) * 23.8 * np.random.uniform(0.95, 1.05, 48)                 # -- Emission Price     [€/t]
    gas = np.ones(48) * 24.8 * np.random.uniform(0.95, 1.05, 48)                # -- Gas Price          [€/MWh]
    lignite = 1.5 * np.random.uniform(0.95, 1.05)                               # -- Lignite Price      [€/MWh]
    coal = 9.9 * np.random.uniform(0.95, 1.05)                                  # -- Hard Coal Price    [€/MWh]
    nuc = 1.0 * np.random.uniform(0.95, 1.05)                                   # -- nuclear Price      [€/MWh]

    prices = dict(power=power_price, gas=gas, co=co, lignite=lignite, coal=coal, nuc=nuc)

    return prices

class PowerPlant(EnergySystem):

    def __init__(self, t=np.arange(24), T=24, dt=1, name='default', power_plant=None):
        super().__init__(t, T, dt)

        # initialize default power plant
        if power_plant is None:
            power_plant = dict(fuel='lignite', maxPower=10, minPower=5, eta=0.4, P0=5, chi=0.407,
                               stopTime=7, runTime=6, gradP=1, gradM=1, on=1, off=0)
        self.power_plant = power_plant
        self.name = name  # power plant block name

        # initialize gurobi model for optimization
        self.m = gby.Model('power_plant')
        self.m.Params.OutputFlag = 0
        self.m.Params.TimeLimit = 30
        self.m.Params.MIPGap = 0.05
        self.m.__len__ = 1

        # determine start up cost
        self.start_cost = 0
        if power_plant['fuel'] == 'gas':
            self.start_cost = 24 * power_plant['maxPower']
        if power_plant['fuel'] == 'lignite':
            if power_plant['maxPower'] > 500:
                self.start_cost = 50 * power_plant['maxPower']
            else:
                self.start_cost = 105 * power_plant['maxPower']
        if power_plant['fuel'] == 'coal':
            if power_plant['maxPower'] > 500:
                self.start_cost = 50 * power_plant['maxPower']
            else:
                self.start_cost = 105 * power_plant['maxPower']
        if power_plant['fuel'] == 'nuc':
            self.start_cost = 50 * power_plant['maxPower']

        self.power = self.emission = self.fuel = self.start = np.zeros_like(self.t, np.float)

    def initialize_model(self, model, name):

        delta = self.power_plant['maxPower'] - self.power_plant['minPower']
        su = self.power_plant['minPower']
        sd = self.power_plant['minPower']

        p_out = model.addVars(self.t, vtype=gby.GRB.CONTINUOUS, name='P_' + name, lb=0, ub=self.power_plant['maxPower'])
        # power at each time t
        model.addConstr(p_out[0] <= self.power_plant['P0'] + self.power_plant['gradP'])
        model.addConstr(p_out[0] >= self.power_plant['P0'] - self.power_plant['gradM'])
        # power corresponding to the optimization
        p_opt = model.addVars(self.t, vtype=gby.GRB.CONTINUOUS, name='opt_', lb=0, ub=delta)

        # states (on, ramp up, ramp down)
        z = model.addVars(self.t, vtype=gby.GRB.BINARY, name='z_' + name)
        v = model.addVars(self.t, vtype=gby.GRB.BINARY, name='v_' + name)
        w = model.addVars(self.t, vtype=gby.GRB.BINARY, name='w_' + name)
        model.addConstrs(p_out[i] == p_opt[i] + z[i] * self.power_plant['minPower'] for i in self.t)

        # power limits
        model.addConstrs(0 <= p_opt[i] for i in self.t)
        model.addConstrs(p_opt[i] <= delta * z[i] for i in self.t)
        model.addConstrs(p_opt[i] <= delta * z[i] - (self.power_plant['maxPower'] - su) * v[i]
                         - (self.power_plant['maxPower'] - sd) * w[i + 1] for i in self.t[:-1])

        # power gradients
        model.addConstrs(p_opt[i] - p_opt[i - 1] <= self.power_plant['gradP'] * z[i - 1] for i in self.t[1:])
        model.addConstrs(p_opt[i - 1] - p_opt[i] <= self.power_plant['gradM'] * z[i] for i in self.t[1:])

        # run- & stop-times
        model.addConstrs(1 - z[i] >= gby.quicksum(w[k] for k in range(max(0, i + 1 - self.power_plant['stopTime']), i))
                         for i in self.t)
        model.addConstrs(z[i] >= gby.quicksum(v[k] for k in range(max(0, i + 1 - self.power_plant['runTime']), i))
                         for i in self.t)
        model.addConstrs(z[i - 1] - z[i] + v[i] - w[i] == 0 for i in self.t[1:])

        # initialize stat
        if self.power_plant['on'] > 0:
            model.addConstrs(z[i] == 1 for i in range(0, self.power_plant['runTime']
                                                      - self.power_plant['on']))
        else:
            model.addConstrs(z[i] == 0 for i in range(0, self.power_plant['stopTime']
                                                      - self.power_plant['off']))
        # fuel cost
        fuel = model.addVars(self.t, vtype=gby.GRB.CONTINUOUS, name='F_' + name, lb=-gby.GRB.INFINITY, ub=gby.GRB.INFINITY)
        if self.power_plant['fuel'] == 'lignite':
            model.addConstrs(fuel[i] == p_out[i] / self.power_plant['eta'] * self.prices['lignite'] for i in self.t)
        if self.power_plant['fuel'] == 'coal':
            model.addConstrs(fuel[i] == p_out[i] / self.power_plant['eta'] * self.prices['coal'] for i in self.t)
        if self.power_plant['fuel'] == 'gas':
            model.addConstrs(fuel[i] == p_out[i] / self.power_plant['eta'] * self.prices['gas'][i] for i in self.t)
        if self.power_plant['fuel'] == 'nuc':
            model.addConstrs(fuel[i] == p_out[i] / self.power_plant['eta'] * self.prices['nuc'] for i in self.t)

        # emission cost
        emission = model.addVars(self.t, vtype=gby.GRB.CONTINUOUS, name='E_' + name, lb=0, ub=gby.GRB.INFINITY)
        model.addConstrs(emission[i] == p_out[i] * self.power_plant['chi'] / self.power_plant['eta']
                         * self.prices['co'][i] for i in self.t)

        # start cost
        start_up = model.addVars(self.t, vtype=gby.GRB.CONTINUOUS, name='S_' + name, lb=0, ub=gby.GRB.INFINITY)
        model.addConstrs(start_up[i] == v[i] * self.start_cost for i in self.t)

        profit = model.addVar(vtype=gby.GRB.CONTINUOUS, name='Profit' + name, lb=-gby.GRB.INFINITY, ub=gby.GRB.INFINITY)
        model.addConstr(profit == gby.quicksum(p_out[i] * self.prices['power'][i] for i in self.t))
        model.setObjective(profit - gby.quicksum(fuel[i] + emission[i] + start_up[i] for i in self.t),
                           gby.GRB.MAXIMIZE)

        model.update()

    def optimize(self):
        self.m.remove(self.m.getVars())
        self.m.remove(self.m.getConstrs())

        self.initialize_model(self.m, self.name)

        self.m.optimize()

        self.power = np.asarray([p.x for p in [x for x in self.m.getVars() if 'P_' in x.VarName]]).reshape((-1,))
        self.fuel = np.asarray([p.x for p in [x for x in self.m.getVars() if 'F_' in x.VarName]]).reshape((-1,))
        self.emission = np.asarray([p.x for p in [x for x in self.m.getVars() if 'E_' in x.VarName]]).reshape((-1,))
        self.start = np.asarray([p.x for p in [x for x in self.m.getVars() if 'S_' in x.VarName]]).reshape((-1,))
        self.generation['power' + self.power_plant['fuel'].capitalize()] = self.power

        return self.power


if __name__ == "__main__":

    prices = get_prices()

    results = {offset: {} for offset in [-7, -5, 0, 5, 7]}

    prevent_starts = {offset: {} for offset in [-7, -5, 0, 5, 7]}
    last = 0

    for offset in [-7, -5, 0, 5, 7]:

        pw1 = PowerPlant(t=np.arange(24), T=24, dt=1)
        obj1 = 0
        pw2 = PowerPlant(t=np.arange(48), T=48, dt=1)
        obj2 = 0

        pr1 = dict(power=prices['power'][:24] + offset, gas=prices['gas'][:24], co=prices['co'][:24],
                   lignite=prices['lignite'], coal=prices['coal'], nuc=prices['nuc'])

        pr2 = dict(power=prices['power'][24:] + offset, gas=prices['gas'][:24], co=prices['co'][24:],
                   lignite=prices['lignite'], coal=prices['coal'], nuc=prices['nuc'])

        pr12 = dict(power=prices['power'] + offset, gas=prices['gas'], co=prices['co'],
                    lignite=prices['lignite'], coal=prices['coal'], nuc=prices['nuc'])

        pw1.set_parameter(date='2018-01-01', weather=None, prices=pr1)
        power1 = pw1.optimize()
        emission1 = pw1.emission
        fuel1 = pw1.fuel
        start1 = pw1.start
        obj1 += pw1.m.getObjective().getValue()

        pw1.power_plant['P0'] = pw1.m.getVarByName('P_%s[%i]' % ('default', 23)).x
        z = np.asanyarray([pw1.m.getVarByName('z_%s[%i]' % ('default', i)).x for i in pw1.t[:24]], np.float)
        if z[-1] > 0:
            index = -1 * pw1.power_plant['runTime']
            pw1.power_plant['on'] = np.count_nonzero(z[index:])
            pw1.power_plant['off'] = 0
        else:
            index = -1 * pw1.power_plant['stopTime']
            pw1.power_plant['off'] = np.count_nonzero(1 - z[index:])
            pw1.power_plant['on'] = 0

        pw1.set_parameter(date='2018-01-02', weather=None, prices=pr2)
        power1 = np.concatenate((power1, pw1.optimize()))
        fuel1 = np.concatenate((fuel1, pw1.fuel))
        emission1 = np.concatenate((emission1, pw1.emission))
        start1 = np.concatenate((start1, pw1.start))
        obj1 += pw1.m.getObjective().getValue()

        results[offset].update({'opt1': (power1,
                                         emission1,
                                         fuel1,
                                         start1,
                                         obj1)})

        pw2.set_parameter(date='2018-01-01', weather=None, prices=pr12)
        power2 = pw2.optimize()
        obj2 = pw2.m.getObjective().getValue()

        results[offset].update({'opt2': (pw2.power,
                                         pw2.emission,
                                         pw2.fuel,
                                         pw2.start,
                                         obj2)})

        delta = obj2 - obj1

        if power1[23] == 0:
            hours = np.argwhere(power1[:24] == 0).reshape((-1,))
            prevent_start = all(power2[hours] > 0)
            if prevent_start:
                prevent_starts.update({offset: (prevent_start, obj1, obj2, delta-last, hours)})
                last = delta
            else:
                prevent_starts.update({offset: (prevent_start, obj1, obj2, delta - last, hours)})
        else:
            prevent_starts.update({offset: (False, obj1, obj2, 0, np.argwhere(power1[:24] == 0).reshape((-1,)))})

    # build orders
    order_book = {}
    last_power = np.zeros(24)                       # last known power
    block_number = 0                                # block number counter
    links = {i: 'x' for i in range(24)}             # current links between blocks
    name = 'test'                                   # agent name

    prevent_start_orders = {}

    for key, value in results.items():      # -7, -5, ...
        result = value['opt1']
        # build mother order if any power > 0 for the current day and the last known power is total zero
        if np.count_nonzero(result[0][:24]) > 0 and np.count_nonzero(last_power) == 0:
            # calculate variable cost for each hour
            var_cost = np.nan_to_num((result[1][:24] + result[2][:24] + result[3][:24]) / result[0][:24])
            # and get mean value for requested price
            price = np.mean(var_cost[var_cost > 0])
            # for each hour with power > 0 add order to order_book
            for hour in np.argwhere(result[0][:24] > 0).reshape((-1,)):
                price = np.round(price, 2)
                power = np.round(result[0][hour])
                order_book.update({str(('gen0', hour, 0, name)): (price, power, 0)})
                links.update({hour: block_number})
            block_number += 1                        # increment block number
            last_power = result[0][:24]              # set last_power to current power

        if prevent_starts[key][0]:
            result = value['opt2']
            hours = prevent_starts[key][4]
            factor = prevent_starts[key][3] / np.sum(result[0][hours])
            # for each hour with power > 0 add order to order_book
            link = 0
            if len(prevent_start_orders) == 0:
                for hour in hours:
                    price = (result[1][hour] + result[2][hour] + result[3][hour]) / result[0][hour]
                    price = np.round(price - factor, 2)
                    power = np.round(result[0][hour])
                    prevent_start_orders.update({str(('gen%s' % block_number, hour, 0, name)): (price, power, link)})
                    order_book.update({str(('gen%s' % block_number, hour, 0, name)): (price, power, link)})
                    link = block_number
                    links.update({hour: block_number})
                    block_number += 1  # increment block number
            else:
                for hour in hours:
                    for key, order in prevent_start_orders.items():
                        if key[1] == hour:
                            order = {key: (np.round(order[0] - factor, 2),
                                           np.round(result[0][hour], 2),
                                           order[2])}
                            prevent_start_orders.update(order)
                            order_book.update(order)

            last_power = result[0][:24]
            # result = value['opt1']

        # add linked hour blocks
        # check if current power is higher then the last known power
        if np.count_nonzero(result[0][:24] - last_power) > 0:
            delta = result[0][:24] - last_power                              # get deltas
            stack_vertical = np.argwhere(last_power > 0).reshape((-1,))     # and check if last_power > 0
            # for each power with last_power > 0
            for hour in stack_vertical:
                # check if delta > 0
                if delta[hour] > 0:
                    # calculate variable cost for the hour and set it as requested price
                    price = np.round((result[1][hour] + result[2][hour]) / result[0][hour], 2)
                    power = np.round(0.2 * delta[hour], 2)
                    # check if the last linked block for this hour is unknown
                    if links[hour] == 'x':
                        link = 0                                            # if unknown, link to mother order
                    else:
                        link = links[hour]                                  # else link to last block for this hour
                    # split volume in five orders and add them to order_book
                    for order in range(5):
                        order_book.update({str(('gen%s' % block_number, hour, order, name)): (price,
                                                                                              power,
                                                                                              link)})
                    links.update({hour: block_number})                      # update last known block for hour
                    block_number += 1                                       # increment block number

            left = stack_vertical[0]            # get first left hour from last_power   ->  __|-----|__
            right = stack_vertical[-1]          # get first right hour from last_power  __|-----|__ <--

            # if the left hour differs from first hour of the current day
            if left > 0:
                # build array for e.g. [0,1,2,3,4,5, ..., left]
                stack_left = np.arange(start=left - 1, stop=-1, step=-1)
                # check if the last linked block for the fist left hour is unknown
                # (only first hour is connected to mother)
                if links[stack_left[0]] == 'x':
                    link = 0                                        # if unknown, link to mother order
                else:
                    link = links[stack_left[0]]                     # else link to last block for this hour
                # for each hour in left_stack
                for hour in stack_left:
                    # check if delta > 0
                    if delta[hour] > 0:
                        # calculate variable cost for the hour and set it as requested price
                        price = np.round((result[1][hour] + result[2][hour]) / result[0][hour], 2)
                        power = np.round(0.2 * delta[hour], 2)
                        # split volume in five orders and add them to order_book
                        for order in range(5):
                            order_book.update({str(('gen%s' % block_number, hour, order, name)): (price,
                                                                                                  power,
                                                                                                  link)})
                        link = block_number
                        links.update({hour: block_number})                  # update last known block for hour
                        block_number += 1                                   # increment block number

            # if the right hour differs from last hour of the current day
            if right < 23:
                # build array for e.g. [right, ... ,19,20,21,22,23]
                stack_right = np.arange(start=right + 1, stop=24)
                # check if the last linked block for the fist right hour is unknown
                # (only first hour is connected to mother)
                if links[stack_right[0]] == 'x':
                    link = 0                                        # if unknown, link to mother order
                else:
                    link = links[stack_right[0]]                    # else link to last block for this hour
                for hour in stack_right:
                    # check if delta > 0
                    if delta[hour] > 0:
                        # calculate variable cost for the hour and set it as requested price
                        price = np.round((result[1][hour] + result[2][hour]) / result[0][hour], 2)
                        power = np.round(0.2 * delta[hour], 2)
                        # split volume in five orders and add them to order_boo
                        for order in range(5):
                            order_book.update({str(('gen%s' % block_number, hour, order, name)): (price,
                                                                                                  power,
                                                                                                  link)})
                        link = block_number
                        links.update({hour: block_number})                  # update last known block for hour
                        block_number += 1                                   # increment block number

            last_power = result[0][:24]                                      # set last_power to current power