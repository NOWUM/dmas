from apps.market import dayAhead_clearing
import configparser
import pandas as pd
import time as tm
import random
from interfaces.interface_mongo import mongoInterface
import multiprocessing
from joblib import Parallel, delayed


def getOrders(id, date, database='MAS_2020', host='149.201.88.150'):
    wait = True                     # Warte solange bis Gebot vorliegt
    start = tm.time()               # Startzeitpunkt
    orders = {}
    i = 0

    config = configparser.ConfigParser()
    config.read('app.cfg')
    database = config['Results']['Database']
    mongoCon = mongoInterface(host=config['MongoDB']['Host'], database=database)

    while wait:
        x = mongoCon.orderDB[date].find_one({"_id": id})  # Abfrage der Gebote
        # Wenn das Gebot vorliegt, füge es hinzu
        if x is not None:
            if 'DayAhead' in x.keys():
               for hour in range(24):
                    dict_ = x['DayAhead']['h_%s' % hour]
                    num_ = len(dict_['price'])
                    for k in range(num_):
                        orders[i] = {'price': dict_['price'][k], 'quantity': dict_['quantity'][k],
                                         'name': id, 'hour': hour}
                        i += 1
                    wait = False  # Warten beenden
                    continue
            else:
                pass
        else:
            tm.sleep(0.05)
        end = tm.time()  # aktueller Zeitstempel
        if end - start >= 120:  # Warte maximal 120 Sekunden
            print('get no orders of Agent %s' % id)
            wait = False
    return orders

def dayAheadClearing(connectionMongo, influx, date):

    num_cores = min(multiprocessing.cpu_count(), 24)
    # Abfrage der anmeldeten Agenten
    agent_ids = connectionMongo.status.find().distinct('_id')
    random.shuffle(agent_ids)

    totalOrders = Parallel(n_jobs=num_cores)(delayed(getOrders)(i, str(date.date())) for i in agent_ids)

    totalDict = {}
    index = 0
    for element in totalOrders:
        for key, value in element.items():
            totalDict[index] = value
            index += 1

    totalOrders = pd.DataFrame.from_dict(totalDict, "index")
    totalOrders = totalOrders.set_index('hour', drop=True)

    hourlyOrders = []
    for i in range(24):
        hourlyOrder = totalOrders[totalOrders.index == i]
        hourlyOrder.index = [k for k in range(len(hourlyOrder))]
        hourlyOrders.append(hourlyOrder.to_dict())

    processed_list = Parallel(n_jobs=num_cores)(delayed(dayAhead_clearing)(hourlyOrder) for hourlyOrder in hourlyOrders)

    time = date
    json_body = []

    for element in processed_list:

        ask = pd.DataFrame.from_dict(element[0])
        bid = pd.DataFrame.from_dict(element[1])
        mcp = element[2]

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
        time += pd.DateOffset(hours=1)

    influx.saveData(json_body)