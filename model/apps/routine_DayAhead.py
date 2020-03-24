from apps.market import dayAhead_clearing
import pandas as pd


def dayAheadClearing(sqlite, influx, date):

    df = pd.DataFrame(columns=['name', 'hour', 'price', 'quantity'])
    agents = sqlite.getAllAgents()

    for name in agents:
        orders = []
        while len(orders) == 0:
            orders = sqlite.getDayAhead(name)
            df = df.append(pd.DataFrame(data=orders, columns=['name', 'hour', 'price', 'quantity']))

    df = df.set_index('hour', drop=True)

    for i in range(24):

        time = date + pd.DateOffset(hours=i)
        o = df[df.index == i]

        ask, bid, mcp, mcm, merit_order = dayAhead_clearing(o, plot=False)
        buy = merit_order.loc[:, ['buy', 'buyNames']]
        sell = merit_order.loc[:, ['sell', 'sellNames']]

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
        id_ = 0
        for row in buy.iterrows():
            json_body.append(
                {
                    "measurement": 'DayAhead',
                    "tags": dict(agent=row[1].buyNames, order='bid', id=id_),
                    "time": time.isoformat() + 'Z',
                    "fields": dict(power=float(row[0]), price=float(row[1].buy))
                }
            )
            id_ += 1

        id_ = 0
        for row in sell.iterrows():
            json_body.append(
                {
                    "measurement": 'DayAhead',
                    "tags": dict(agent=row[1].sellNames, order='ask', id=id_),
                    "time": time.isoformat() + 'Z',
                    "fields": dict(power=float(row[0]), price=float(row[1].sell))
                }
            )
            id_ += 1

        json_body.append(
            {
                "measurement": 'DayAhead',
                "time": time.isoformat() + 'Z',
                "fields": dict(price=mcp)
            }
        )
        influx.influx.write_points(json_body)
