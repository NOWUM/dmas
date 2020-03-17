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

# Reservemarkt
def balancing_clearing(orders, ask=1800, minimal=5):

    orders = orders.sort_values(by=['price'])
    orders['quantity'] = orders['quantity'].cumsum()
    orders = orders.set_index('quantity')
    namesOrders = set(orders['name'].to_numpy())

    result = orders[orders.index <= ask]

    if len(result) == 0:
        result = orders.iloc[[0], :]
        result.loc[result.index[0],'volume'] = ask
        result.set_index('name', inplace=True)
        diff = list(namesOrders.difference(set(result.index)))
        for n in diff:
            result.loc[n] = 0

        return result

    rest = ask - result.index[-1]

    if rest >= minimal:
        df = orders.loc[(orders.index > result.index[-1])]
        if len(df) > 0:
            result = result.append(df.iloc[0,:])
            lst = list(result.index[:-1])
            lst.append(ask)
            result.index = lst
    else:
        df = orders.loc[(orders.index > result.index[-1])]
        if len(df) > 0:
            result = result.append(df.iloc[0,:])
            lst = list(result.index[:-1])
            lst.append(lst[-1]+minimal)
            result.index = lst

    volume = result.index
    result.set_index('name', inplace=True)
    result['volume'] = np.diff(volume.insert(0,0))
    result = result.groupby(['name']).sum()

    diff = list(namesOrders.difference(set(result.index)))
    for n in diff:
        result.loc[n] = 0

    return result

# Day Ahead Markt
def dayAhead_clearing(orders, plot=False):

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

    askMax = ask0['mo'].max()
    if math.isnan(askMax):
        askMax=0
    bidMax = bid0['mo'].max()
    if math.isnan(bidMax):
        bidMax=0

    #maxi = max(askMax, bidMax)
    #index = np.arange(0, maxi+0.1, 0.1)
    #index = [np.round(i,1) for i in index]
    merit_order = pd.DataFrame(index=np.concatenate((ask0.index,bid0.index)))
    # merit_order = pd.DataFrame()
    merit_order.loc[:,'buy'] = bid0['price']
    merit_order.loc[:,'buyNames'] = bid0['name']
    merit_order.loc[:,'sellNames'] = ask0['name']
    merit_order.loc[:,'sell'] = ask0['price']

    merit_order = merit_order.sort_index()
    merit_order = merit_order.bfill()
    merit_order = merit_order.dropna()

    index = list(np.round((np.diff((merit_order.index))),1))
    index.insert(0,merit_order.index[0])
    merit_order.index = index

    if plot:
        merit_order.plot()

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

    return ask_last, bid_last, mcp, mcm, merit_order

if __name__ == "__main__":

    df2 = orderGen(150)
    df2 = df2[df2['quantity'] <=0]

    test = [(0, -300, 'Nils'), (3000, 3000, 'Nils')]
    df = pd.DataFrame(test, columns=['quantity', 'price', 'name'])
    #df2 = df2.append(df)
    df = pd.read_excel(r'./tmp/DA_2019-01-02_20.xlsx', index_col=0)
    #df = df[df.index == 5]
    #df = df[df['quantity'] != 0]

    # df = pd.read_excel(r'./tmp/DA_2019-01-01_5.xlsx', index_col=0)
    #ask, bid, mcp, mcm, test = dayAhead_clearing(df, plot=True)
    ask_last, bid_last, mcp, mcm, merit_order = dayAhead_clearing(df)
    #df = pd.read_excel(r'./tmp/test.xlsx', index_col=0)
    #df = df[df['typ'] == 'neg']
    #del df['typ']
    #df['quantity'] = df['quantity'].to_numpy(dtype=float)
    #print(df)
    # df = df[df['quantity'] > 0]
    # actual = pd.read_excel(r'./tmp/actual.xlsx', index_col=0)

    # ask = np.sum(actual['quantity'].to_numpy())
    # df = pd.read_excel(r'./tmp/test.xlsx', index_col=0)

    # test = balancing_clearing(orders=df, ask=np.abs(np.round(-197.14000000000001,1)), minimal=0)
    #print(test)