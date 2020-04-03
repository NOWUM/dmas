from apps.market import balancing_clearing
import pandas as pd
import numpy as np
import time as tm

def balPowerClearing(connectionMongo, influx, date, power=1800):
    # Dataframe für alle Gebote der Agenten
    df = pd.DataFrame(columns=['name', 'slot', 'quantity', 'typ', 'powerPrice', 'energyPrice'])
    # Abfrage der anmeldeten Agenten
    agent_ids = connectionMongo.status.find().distinct('_id')
    # Sammel für jeden Agent die Gebote
    for id in agent_ids:
        # Wenn der Agent Regelleistung bereitstellt
        if connectionMongo.status.find_one({"_id": id})['reserve']:
            print('waiting for Agent %s' % id)
            wait = True                                                                 # Warte solange bis Gebot vorliegt
            start = tm.time()                                                           # Startzeitpunkt
            while wait:
                x = connectionMongo.orderDB[str(date.date())].find_one({"_id": id})     # Abfrage der Gebote
                if x is not None:
                    if 'Balancing' in x.keys():
                        # Wenn das Gebot vorliegt, füge es hinzu
                        orders = pd.DataFrame.from_dict(x['Balancing'], 'index')
                        df = df.append(orders)
                        wait = False                                                        # Warten beenden
                        continue
                else:
                    tm.sleep(0.05)
                end = tm.time()  # aktueller Zeitstempel
                if end - start >= 30:                                                   # Warte maximal 30 Sekunden
                    print('get no orders of Agent %s' % id)
                    wait = False
    df = df.set_index('slot', drop=True)
    df = df.rename(columns={'powerPrice': 'price'})

    # Für postive und negative Regelleistung
    for balancing in ['pos', 'neg']:
        # bestimme für jeden Block die Zuschläge
        for i in range(6):
            # Alle Gebote des jeweiligen Slots & Typs
            marketInput = df.loc[(df['typ'] == balancing) & (df.index == i), ['name', 'quantity', 'price']]
            # Führe das Marktclearing durch
            result = balancing_clearing(marketInput, ask=power, minimal=5)
            # Erstelle Preisinfomationen und speichere diese in der Influx
            prices = df.loc[df['typ'] == balancing, ['name', 'price', 'energyPrice']]
            prices = prices.loc[i, :]
            prices = prices.set_index('name', drop=True)
            # Zeitstempel des Blockgebotes
            time = date + pd.DateOffset(hours=i * 4)
            json_body = []
            for r in result.index:
                json_body.append(
                    {
                        "measurement": 'Balancing',
                        "tags": dict(agent=r, area=r.split('_')[-1], typ=r.split('_')[0], order=balancing),
                        "time": time.isoformat() + 'Z',
                        "fields": dict(maxPrice=max(result['price'].to_numpy()), power=result.loc[r, 'volume'],
                                       energyPrice=prices.loc[r, 'energyPrice'], powerPrice=prices.loc[r, 'price'])
                    }
                )
            influx.saveData(json_body)

def balEnergyClearing(connectionMongo, influx, date):
    # Dataframe für alle Abweichungen der Agenten
    df = pd.DataFrame(columns=['name', 'hour', 'quantity'])
    # Abfrage der anmeldeten Agenten
    agent_ids = connectionMongo.status.find().distinct('_id')
    # Sammel für jeden Agent die Fahrplanabweichungen
    for id in agent_ids:
        print('waiting for Agent %s' % id)
        wait = True                                                                     # Warte solange bis Gebot vorliegt
        start = tm.time()  # Startzeitpunkt
        while wait:
            x = connectionMongo.orderDB[str(date.date())].find_one({"_id": id})         # Abfrage der Gebote
            if x is not None:
                if 'Actual' in x.keys():
                    # Wenn das Gebot vorliegt, füge es hinzu
                    orders = pd.DataFrame.from_dict(x['Actual'], 'index')
                    df = df.append(orders)
                    wait = False  # Warten beenden
                    continue
            else:
                tm.sleep(0.05)
            end = tm.time()  # aktueller Zeitstempel
            if end - start >= 30:                                                       # Warte maximal 30 Sekunden
                print('get no Actuals of Agent %s' % id)
                wait = False

    # Abfrage der Kosten, die durch die Bereitstellung der Leistung entstanden sind
    fees = influx.getBalancingPowerFees(date)                                           # positiv wie negative Leistung

    # Abfrage der leistungsbezuschlagten Agenten
    balAgents = [id for id in agent_ids if connectionMongo.status.find_one({"_id": id})['reserve']]
    orders = influx.getBalEnergy(date, balAgents)

    df = df.set_index('hour', drop=True)
    slot = 0                                                                            # Slot Indikator
    first = True                                                                        # Bool für den ersten Durchlauf

    # Berechene für jede Stunden den Regelenergieabruf
    for i in range(24):

        typ = 'pos'
        actual = df[df.index == i]
        ask = np.sum(actual['quantity'].to_numpy())
        if ask < 0:
            typ = 'neg'

        order = orders[orders.index == slot]
        order = order.loc[order['typ'] == typ, ['name', 'quantity', 'price']]
        result = balancing_clearing(order, np.abs(np.round(ask,1)), minimal=0)

        for name in result.index:
            result.loc[name,'price'] = order.loc[order['name'] == name, 'price'].to_numpy()[0]

        totalFees = fees[slot] + sum(result['price'].to_numpy(dtype=float) * result['volume'].to_numpy(dtype=float))

        reBAP = totalFees/np.sum(np.abs(actual['quantity'].to_numpy(dtype=float)))
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

        influx.saveData(json_body)

        if i % 4 == 0:
            if first: first = False
            else: slot += 1