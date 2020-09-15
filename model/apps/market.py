# Importe
import numpy as np
import pandas as pd
import math
import time


def orderGen(n):
    df1 = pd.DataFrame()
    df1['00:00'] = np.random.uniform(-10, 10, size=n) * 50
    df2 = pd.DataFrame()
    df2['00:00'] = np.random.uniform(0, 50, size=n)
    df3 = pd.DataFrame()
    names = ['Nils', 'Jochen', 'Christian', 'Kevin', 'Andre', 'Jörg', 'Svea', 'Isabel', 'Dominik', 'Markus',
             'Denise']
    df3['00:00'] = np.random.choice(names, size=n)
    df4 = df1
    df4['price'] = df2
    df4['name'] = df3
    df4 = df4[df4['00:00'] != 0]
    df4.columns = ['quantity', 'price', 'name']

    return df4

# Day Ahead Markt
def dayAhead_clearing(orders):

    orders = pd.DataFrame.from_dict(orders)

    ask0 = orders[orders['quantity'] < -0.1]
    ask0 = ask0.sort_values(by=['price'])
    ask0.loc[:,['quantity']] = -1*ask0['quantity'].round(1)
    ask0.insert(1,'mo',np.round(ask0['quantity'].cumsum(),1))
    ask0 = ask0.set_index(ask0['mo'])
    namesAsk = set(ask0['name'].to_numpy())

    bid0 = orders[orders['quantity'] > 0.1]
    bid0 = bid0.sort_values(by=['price'], ascending=False)
    bid0.loc[:,['quantity']] = bid0['quantity'].round(1)
    bid0.insert(1,'mo', np.round(bid0['quantity'].cumsum(),1))
    bid0 = bid0.set_index(bid0['mo'])
    namesBid = set(bid0['name'].to_numpy())

    if bid0['mo'].max() >= ask0['mo'].max():
        diff = bid0['mo'].max() - ask0['mo'].max()
        maxPrice = ask0['price'].max() + 1
        df = pd.DataFrame(index=[diff + ask0['mo'].max()],
                          data=dict(quantity=diff, mo=diff + ask0['mo'].max(), price=maxPrice, name='extra'))
        ask0 = ask0.append(df, sort=True)

    merit_order = pd.DataFrame(index=np.concatenate((ask0.index, bid0.index)))

    merit_order.loc[:, 'buy'] = bid0['price']
    merit_order.loc[:, 'buyNames'] = bid0['name']
    merit_order.loc[:, 'sellNames'] = ask0['name']
    merit_order.loc[:, 'sell'] = ask0['price']

    merit_order = merit_order.sort_index()
    merit_order = merit_order.bfill()
    merit_order = merit_order.dropna()


    index = list(np.round((np.diff((merit_order.index))),1))
    index.insert(0, merit_order.index[0])
    merit_order.index = index

    if ask0['price'].min() > bid0['price'].max():
        mcp = np.nan
        mcm = 0
        ask_last = pd.DataFrame()
        bid_last = pd.DataFrame()

    else:
        # Clearing Ergebnisse
        mcp = merit_order.loc[merit_order['sell'] <= merit_order['buy']]['sell'].max()

        buyers = np.unique(merit_order['buyNames'])
        buyVolumes = [sum(merit_order[merit_order['buyNames'] == name].index) for name in buyers]
        sellers = np.unique(merit_order['sellNames'])
        sellVolume = [sum(merit_order[merit_order['sellNames'] == name].index) for name in sellers]

        ask_last = pd.DataFrame(index=sellers, data=sellVolume, columns=['volume'])
        bid_last = pd.DataFrame(index=buyers, data=buyVolumes, columns=['volume'])

        mcm = ask_last.sum().volume

    diff = list(namesAsk.difference(set(ask_last.index)))
    for n in diff:
        ask_last.loc[n] = 0

    diff = list(namesBid.difference(set(bid_last.index)))
    for n in diff:
        bid_last.loc[n] = 0

    result = (ask_last.to_dict(), bid_last.to_dict(), mcp, mcm)

    return result


if __name__ == "__main__":

    df2 = orderGen(1000)
    result = dayAhead_clearing(df2)




