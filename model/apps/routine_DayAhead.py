from apps.market import dayAhead_clearing
import pandas as pd
import time as tm

def dayAheadClearing(connectionMongo, influx, date):
    # Dataframe für alle Gebote der Agenten
    df = pd.DataFrame(columns=['name', 'hour', 'price', 'quantity'])
    # Abfrage der anmeldeten Agenten
    agent_ids = connectionMongo.tableOrderbooks.find().distinct('_id')
    # Sammel für jeden Agent die Gebote
    for id in agent_ids:
        print('waiting for Agent %s' % id)
        wait = True                                                         # Warte solange bis Gebot vorliegt
        start = tm.time()                                                   # Startzeitpunkt
        while wait:
            x = connectionMongo.tableOrderbooks.find_one({"_id": id})       # Abfrage der Gebote
            # Wenn das Gebot vorliegt, füge es hinzu
            if str(date.date()) in x.keys():
                for hour in range(24):
                    dict_ = x[str(date.date())]['DayAhead']['h_%s' %hour]
                    num_ = len(dict_['price'])
                    orders = pd.DataFrame({'price': dict_['price'], 'quantity': dict_['quantity'],
                                           'name': [id for _ in range(num_)], 'hour': [hour for _ in range(num_)]})
                    df = df.append(orders)
                wait = False                                                # Warten beenden
            else:
                tm.sleep(0.2)
            end = tm.time()  # aktueller Zeitstempel
            if end - start >= 30:                                           # Warte maximal 30 Sekunden
                print('get no orders of Agent %s' % id)
                wait = False

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
