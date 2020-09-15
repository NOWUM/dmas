# third pary modules
import configparser
import pandas as pd
import time as tme
import random
import multiprocessing
from joblib import Parallel, delayed

# model modules
from apps.market import dayAhead_clearing
from interfaces.interface_mongo import mongoInterface


def get_orders(name, date):

    # initialize connection to mongodb to get the orders
    config = configparser.ConfigParser()
    config.read('app.cfg')
    database = config['Results']['Database']
    mon_db = mongoInterface(host=config['MongoDB']['Host'], database=database)
    orders = {}                     # all orders from agent
    i = 0                           # order counter (key in dictionary)
    wait = True                     # check if orders from agent are delivered
    start = tme.time()              # start waiting
    # start collecting orders
    while wait:
        x = mon_db.orderDB[date].find_one({"_id": name})
        if x is not None:
            if 'DayAhead' in x.keys():
                # build dictionary with all day ahead orders
                for hour in range(24):
                    dict_ = x['DayAhead']['h_%s' % hour]
                    num_ = len(dict_['price'])
                    for k in range(num_):
                        orders[i] = {'price': dict_['price'][k], 'quantity': dict_['quantity'][k],
                                         'name': name, 'hour': hour}
                        i += 1
                    wait = False
                    continue
            else:
                pass
        else:
            tme.sleep(1)            # wait a second and ask mongodb again
        end = tme.time()            # aktueller Zeitstempel
        if end - start >= 120:      # wait maximal 120 seconds
            print('get no orders of Agent %s' % name)
            wait = False

    mon_db.mongo.close()            # close connection to mongodb
    return orders


def da_clearing(mongo_con, influx_con, date):

    num_cores = min(multiprocessing.cpu_count(), 6)
    agent_ids = mongo_con.status.find().distinct('_id')               # all logged in Agents
    random.shuffle(agent_ids)                                         # shuffle ids to prevent long wait
    # get orders for each agent
    total_orders = Parallel(n_jobs=num_cores)(delayed(get_orders)(i, str(date.date())) for i in agent_ids)

    # build data frame for market clearing calculation
    total_dict = {}
    index = 0
    for element in total_orders:
        for key, value in element.items():
            total_dict[index] = value
            index += 1

    total_orders = pd.DataFrame.from_dict(total_dict, "index")
    total_orders = total_orders.set_index('hour', drop=True)

    # calculate market clearing for each hour
    hourly_orders = []
    for i in range(24):
        hourly_order = total_orders[total_orders.index == i]
        hourly_order.index = [k for k in range(len(hourly_order))]
        hourly_orders.append(hourly_order.to_dict())

    processed_list = Parallel(n_jobs=num_cores)(delayed(dayAhead_clearing)(hourlyOrder) for hourlyOrder in hourly_orders)

    # save result in influxdb
    time = date
    for element in processed_list:
        # save all asks
        ask = pd.DataFrame.from_dict(element[0])
        ask.columns = ['power']
        ask['names'] = [name for name in ask.index]
        ask['order'] = ['ask' for _ in range(len(ask))]
        ask['typ'] = [name.split('_')[0] for name in ask['names'].to_numpy()]
        ask.index = [time for _ in range(len(ask))]
        influx_con.influx.write_points(dataframe=ask, measurement='DayAhead', tag_columns=['names', 'order', 'typ'])
        # save all bids
        bid = pd.DataFrame.from_dict(element[1])
        bid.columns = ['power']
        bid['names'] = [name for name in bid.index]
        bid['order'] = ['bid' for _ in range(len(bid))]
        bid['typ'] = [name.split('_')[0] for name in bid['names'].to_numpy()]
        bid.index = [time for _ in range(len(bid))]
        influx_con.influx.write_points(dataframe=bid, measurement='DayAhead', tag_columns=['names', 'order', 'typ'])
        # save mcp
        mcp = pd.DataFrame(data=[element[2]], index=[time], columns=['price'])
        influx_con.influx.write_points(dataframe=mcp, measurement='DayAhead')
        # next hour
        time += pd.DateOffset(hours=1)


if __name__ == "__main__":

    date = pd.to_datetime('2018-01-28')
    config = configparser.ConfigParser()  # read config file
    config.read('app.cfg')

    database = config['Results']['Database']  # name of influxdatabase to store the results
    mongo_con = mongoInterface(host=config['MongoDB']['Host'], database=database)  # connection and interface to MongoDB
    # influxCon = influxCon(host=config['InfluxDB']['Host'], database=database)  # connection and interface to InfluxDB

    num_cores = min(multiprocessing.cpu_count(), 6)
    agent_ids = mongo_con.status.find().distinct('_id')               # all logged in Agents
    random.shuffle(agent_ids)                                         # shuffle ids to prevent long wait
    # get orders for each agent
    total_orders = Parallel(n_jobs=num_cores)(delayed(get_orders)(i, str(date.date())) for i in agent_ids)

