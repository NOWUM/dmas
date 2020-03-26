from apps.market import balancing_clearing
import pandas as pd
import numpy as np
import time as tm

def balPowerClearing(sqlite, influx, date, power=1800):

    df = pd.DataFrame(
        columns=['name', 'slot', 'quantity', 'typ', 'powerPrice', 'energyPrice'])  # -- df to save all orders
    agents = sqlite.getBalancingAgents()  # -- get all agents
    lastName = ''
    timeouts = []

    for name in agents:
        orders = []
        start = tm.time()
        while len(orders) == 0:
            if name != lastName:
                print('waiting for orders of Agent %s' %name)
                lastName = name
            orders = sqlite.getBalancing(name)
            end = tm.time()
            df = df.append(
                pd.DataFrame(data=orders, columns=['name', 'slot', 'quantity', 'typ', 'powerPrice', 'energyPrice']))
            if end - start >= 30:
                timeouts.append(name)
                print('get no orders of Agent %s' %name)
                break

    for name in timeouts:
        print('last chance for Agent %s' %name)
        orders = sqlite.getBalancing(name)
        df = df.append(
            pd.DataFrame(data=orders, columns=['name', 'slot', 'quantity', 'typ', 'powerPrice', 'energyPrice']))
        if len(orders) > 0:
            print('get orders of Agent %s' %name)
        else:
            print('get no orders of Agent %s' %name)


    df = df.set_index('slot', drop=True)
    df = df.rename(columns={'powerPrice': 'price'})

    for balancing in ['pos', 'neg']:
        for i in range(6):

            time = date + pd.DateOffset(hours=i * 4)
            tmp = df.loc[df['typ'] == balancing, ['name', 'quantity', 'price']]
            o = tmp[tmp.index == i]

            result = balancing_clearing(o, ask=power, minimal=5)

            mcp = max(result['price'].to_numpy())
            prices = df.loc[df['typ'] == balancing, ['name', 'price', 'energyPrice']]
            prices = prices.loc[i, :]
            prices = prices.set_index('name', drop=True)

            json_body = []
            for r in result.index:
                json_body.append(
                    {
                        "measurement": 'Balancing',
                        "tags": dict(agent=r, area=r.split('_')[-1], typ=r.split('_')[0], order=balancing),
                        "time": time.isoformat() + 'Z',
                        "fields": dict(maxPrice=mcp, power=result.loc[r, 'volume'],
                                       energyPrice=prices.loc[r, 'energyPrice'], powerPrice=prices.loc[r, 'price'])
                    }
                )
            influx.influx.write_points(json_body)

def balEnergyClearing(sqlite, influx, date):

    actuals = pd.DataFrame(columns=['name', 'hour', 'quantity'])
    agents = sqlite.getAllAgents()
    lastName = ''
    timeouts = []

    for name in agents:
        orders = []
        start = tm.time()
        while len(orders) == 0:
            if name != lastName:
                print('waiting for orders of Agent %s' %name)
                lastName = name
            orders = sqlite.getActual(name)
            end = tm.time()
            actuals = actuals.append(pd.DataFrame(data=orders, columns=['name', 'hour', 'quantity']))
            if end - start >= 30:
                timeouts.append(name)
                print('get no orders of Agent %s' %name)
                break

    for name in timeouts:
        print('last chance for Agent %s' %name)
        orders = sqlite.getActual(name)
        actuals = actuals.append(pd.DataFrame(data=orders, columns=['name', 'hour', 'quantity']))
        if len(orders) > 0:
            print('get orders of Agent %s' %name)
        else:
            print('get no orders of Agent %s' %name)

    posCost, negCost = influx.getBalPowerCosts(date)
    orders = influx.getBalEnergy(date, sqlite.getBalancingAgents())

    slot = 0; first = True

    for i in range(24):

        typ = 'pos'
        actual = actuals[actuals.index == i]
        ask = np.sum(actual['quantity'].to_numpy())

        if ask < 0: typ = 'neg'

        order = orders[orders.index == slot]
        order = order.loc[order['typ'] == typ, ['name', 'quantity', 'price']]
        result = balancing_clearing(order, np.abs(np.round(ask,1)), minimal=0)

        for name in result.index:
            result.loc[name,'price'] = order.loc[order['name'] == name, 'price'].to_numpy()[0]

        totalCost = (posCost[slot] + negCost[slot]) + \
                    sum(result['price'].to_numpy(dtype=float) * result['volume'].to_numpy(dtype=float))

        reBAP = totalCost/np.sum(np.abs(actual['quantity'].to_numpy(dtype=float)))
        actual.loc[:,'cost'] = np.abs(actual['quantity'].to_numpy(dtype=float)) * reBAP
        actual.set_index('name', inplace=True)
        time = date + pd.DateOffset(hours=i)

        influx.influx.switch_database("MAS_2019")
        json_body = []

        for r in result.index:
           json_body.append(
               {
                   "measurement": 'Balancing',
                   "tags": dict(agent=r, area=r.split('_')[-1], typ=r.split('_')[0], order=typ),
                   "time": time.isoformat() + 'Z',
                   "fields": dict(energy=result.loc[r, 'volume'])
               }
           )
        for r in actual.index:
            json_body.append(
                {
                    "measurement": 'Balancing',
                    "tags": dict(agent=r, area=r.split('_')[-1], typ=r.split('_')[0], order=typ),
                    "time": time.isoformat() + 'Z',
                    "fields": dict(cost=actual.loc[r, 'cost'])
                }
            )

        influx.influx.write_points(json_body)

        if i % 4 == 0:
            if first: first = False
            else: slot += 1