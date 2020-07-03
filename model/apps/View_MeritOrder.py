import numpy as np
import pandas as pd
import plotly.express as px

def getMeritOrder(mongo, day=pd.to_datetime('2019-01-01'), hour=1):

    myMongo = mongo

    allOrders = myMongo.orderDB[str(day)]
    meritOrder = []
    table = 'h_' + str(hour)

    for orders in allOrders.find():
        meritOrder.append(orders['DayAhead'][table])
        bid = []
        ask = []
        for m in meritOrder:
            len_ = len(m['quantity'])
            for i in range(len_):
                try:
                    if m['quantity'][i] >= 0:
                        bid.append((m['quantity'][i], m['price'][i]))
                    elif m['quantity'][i] < 0:
                        ask.append((m['quantity'][i], m['price'][i]))
                except:
                    pass
    bid = np.asarray(bid)
    ask = np.asarray(ask)
    dfBid = pd.DataFrame(data=bid, columns=['volume', 'price'])
    dfAsk = pd.DataFrame(data=ask, columns=['volume', 'price'])

    dfBid = dfBid.sort_values('price', ascending=False)
    dfBid['volume'] = dfBid['volume'].cumsum()
    dfBid.dropna(inplace=True)

    dfAsk = dfAsk.sort_values('price', ascending=True)
    dfAsk['volume'] = -1 * dfAsk['volume'].cumsum()
    dfAsk.dropna(inplace=True)

    lst = []
    for element in dfAsk.iterrows():
        lst.append([element[1].iloc[0], 'ask', element[1].iloc[1]])

    for element in dfBid.iterrows():
        lst.append([element[1].iloc[0], 'bid', element[1].iloc[1]])

    df = pd.DataFrame(data=lst, columns=['volume', 'order', 'price'])

    data = px.line(df, x="volume", y="price", color='order')
    plot = data.to_json()

    return plot

if __name__ == "__main__":

    plot = getMeritOrder()
