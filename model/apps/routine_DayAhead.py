from apps.market import dayAhead_clearing
import pandas as pd
import time as tm

def dayAheadClearing(sqlite, influx, date):

    df = pd.DataFrame(columns=['name', 'hour', 'price', 'quantity'])
    agents = sqlite.getAllAgents()
    lastName = ''
    timeouts = []

    for name in agents:
        orders = []
        start = tm.time()
        while (len(orders) == 0):
            if name != lastName:
                print('waiting for orders of Agent %s' %name)
                lastName = name
            orders = sqlite.getDayAhead(name)
            end = tm.time()
            df = df.append(pd.DataFrame(data=orders, columns=['name', 'hour', 'price', 'quantity']))
            if end - start >= 30:
                timeouts.append(name)
                print('get no orders of Agent %s' %name)
                break

    for name in timeouts:
        print('last chance for Agent %s' %name)
        orders = sqlite.getDayAhead(name)
        df = df.append(pd.DataFrame(data=orders, columns=['name', 'hour', 'price', 'quantity']))
        if len(orders) > 0:
            print('get orders of Agent %s' %name)
        else:
            print('get no orders of Agent %s' %name)

    df = df.set_index('hour', drop=True)

    for i in range(24):

        time = date + pd.DateOffset(hours=i)
        o = df[df.index == i]

        ask, bid, mcp, mcm, _ = dayAhead_clearing(o, plot=False)

        influx.influx.switch_database("MAS_2019")
        json_body = []
        for r in ask.index:
            json_body.append(
                {
                    "measurement": 'DayAhead',
                    "tags": dict(agent=r, order='ask', area=r.split('_')[-1], typ=r.split('_')[0]),
                    "time": time.isoformat() + 'Z',
                    "fields": dict(power=float(ask.loc[r, 'volume']))
                }
            )
        for r in bid.index:
            json_body.append(
                {
                    "measurement": 'DayAhead',
                    "tags": dict(agent=r, order='bid', area=r.split('_')[-1], typ=r.split('_')[0]),
                    "time": time.isoformat() + 'Z',
                    "fields": dict(power=float(bid.loc[r, 'volume']))
                }
            )

        json_body.append(
            {
                "measurement": 'DayAhead',
                "time": time.isoformat() + 'Z',
                "fields": dict(price=mcp)
            }
        )
        influx.influx.write_points(json_body)
