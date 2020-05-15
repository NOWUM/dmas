from apps.market import dayAhead_clearing
import pandas as pd
import time as tm
import random


def dayAheadClearing(connectionMongo, influx, date):
    # Dataframe für alle Gebote der Agenten
    #df = pd.DataFrame(columns=['name', 'hour', 'price', 'quantity'])
    # Abfrage der anmeldeten Agenten
    agent_ids = connectionMongo.status.find().distinct('_id')
    random.shuffle(agent_ids)
    total_dict = {}
    i = 0
    # Sammel für jeden Agent die Gebote
    for id in agent_ids:
        print('waiting for Agent %s' % id)
        wait = True                                                              # Warte solange bis Gebot vorliegt
        start = tm.time()                                                        # Startzeitpunkt
        while wait:
            x = connectionMongo.orderDB[str(date.date())].find_one({"_id": id})  # Abfrage der Gebote
            # Wenn das Gebot vorliegt, füge es hinzu
            if x is not None:
                if 'DayAhead' in x.keys():
                    test = tm.time()
                    for hour in range(24):
                        dict_ = x['DayAhead']['h_%s' % hour]
                        num_ = len(dict_['price'])
                        for k in range(num_):
                            total_dict[i] = {'price': dict_['price'][k], 'quantity': dict_['quantity'][k],
                                                   'name': id, 'hour': hour}
                            i += 1
                    wait = False                                                # Warten beenden
                    continue
            else:
                tm.sleep(0.05)
            end = tm.time()                                                 # aktueller Zeitstempel
            if end - start >= 60:                                           # Warte maximal 30 Sekunden
                print('get no orders of Agent %s' % id)
                wait = False
    df = pd.DataFrame.from_dict(total_dict, "index")
    df = df.set_index('hour', drop=True)

    for i in range(24):

        time = date + pd.DateOffset(hours=i)
        o = df[df.index == i]

        ask, bid, mcp, mcm, _ = dayAhead_clearing(o)

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
        influx.saveData(json_body)
