# third pary modules
import configparser
import pandas as pd
import numpy as np
import time as tme
import random
import multiprocessing
from joblib import Parallel, delayed

# model modules
from apps.market import market
from interfaces.interface_mongo import mongoInterface


def get_orders(name, date):

    # initialize connection to mongodb to get the orders
    config = configparser.ConfigParser()
    config.read('app.cfg')
    database = config['Results']['Database']
    mon_db = mongoInterface(host=config['MongoDB']['Host'], database=database)
    ask_orders = {}                     # all orders from agent
    bid_orders = {}
    typ = 'error'
    wait = True                     # check if orders from agent are delivered
    start = tme.time()              # start waiting
    # start collecting orders
    while wait:
        x = mon_db.orderDB[date].find_one({"_id": name})
        if x is not None:
            if 'DayAhead' in x.keys():
                for key, value in x['DayAhead'].items():
                    key = eval(key)
                    if 'dem' in key[0]:
                        key = (int(key[0].replace('dem', '')), key[1], key[2], key[3])
                        bid_orders.update({key: (value[0], np.abs(value[1]), value[2])})
                    elif 'gen' in key[0]:
                        key = (int(key[0].replace('gen', '')), key[1], key[2], key[3])
                        ask_orders.update({key: (value[0], np.abs(value[1]), value[2])})

                wait = False
            else:
                pass
        else:
            tme.sleep(1)            # wait a second and ask mongodb again
        end = tme.time()            # aktueller Zeitstempel
        if end - start >= 120:      # wait maximal 120 seconds
            print('get no orders of Agent %s' % name)
            wait = False

    mon_db.mongo.close()            # close connection to mongodb
    orders = (ask_orders, bid_orders)
    return orders


def da_clearing(mongo_con, influx_con, date):

    num_cores = min(multiprocessing.cpu_count(), 6)
    agent_ids = mongo_con.status.find().distinct('_id')               # all logged in Agents
    random.shuffle(agent_ids)                                         # shuffle ids to prevent long wait
    # get orders for each agent
    total_orders = Parallel(n_jobs=num_cores)(delayed(get_orders)(i, str(date.date())) for i in agent_ids)

    da_market = market()

    for order in total_orders:
        da_market.set_parameter(ask=order[0], bid=order[1])

    result = da_market.optimize()

    # save result in influxdb
    time = date
    for element in result:
        # save all asks
        ask = pd.DataFrame.from_dict(element[0])
        ask.columns = ['power']
        ask['names'] = [name.split('-')[0] for name in ask.index]
        ask = ask.groupby('names').sum()
        ask['order'] = ['ask' for _ in range(len(ask))]
        ask['typ'] = [name.split('_')[0] for name in ask.index]
        ask['names'] = [name for name in ask.index]
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
        mcp = pd.DataFrame(data=[np.asarray(element[2], dtype=float)], index=[time], columns=['price'])
        influx_con.influx.write_points(dataframe=mcp, measurement='DayAhead')
        # next hour
        time += pd.DateOffset(hours=1)


if __name__ == "__main__":
    pass

    from gurobipy import *

    date = pd.to_datetime('2018-01-01')
    config = configparser.ConfigParser()  # read config file
    config.read('app.cfg')

    database = config['Results']['Database']  # name of influxdatabase to store the results
    mongo_con = mongoInterface(host=config['MongoDB']['Host'], database=database)  # connection and interface to MongoDB
    # # influxCon = influxCon(host=config['InfluxDB']['Host'], database=database)  # connection and interface to InfluxDB
    #
    num_cores = min(multiprocessing.cpu_count(), 6)
    agent_ids = mongo_con.status.find().distinct('_id')               # all logged in Agents
    # #random.shuffle(agent_ids)                                         # shuffle ids to prevent long wait
    # # get orders for each agent
    total_orders = Parallel(n_jobs=num_cores)(delayed(get_orders)(i, str(date.date())) for i in agent_ids)
    #
    da_market = market()
    for order in total_orders:
        da_market.set_parameter(ask=order[0], bid=order[1])

    result = da_market.optimize()

    # ask_id, ask_prc, ask_vol, ask_block = multidict(da_market.ask_orders)
    # # get all ask agents
    # ask_agents = np.unique([a[-1] for a in ask_id])
    # ask_blocks = tuplelist([(i, agent, ask_block.select(i, '*', '*', str(agent))[0]) for agent in ask_agents
    #                         for i in range(ask_id.select('*', '*', '*', str(agent))[-1][0] + 1) if ask_block.select(i, '*', '*', str(agent))[0] != 'x'])
    #
    # for agent in ask_agents:
    #     num = ask_id.select('*', '*', '*', str(agent))[-1][0] + 1
    #     for i in range(ask_id.select('*', '*', '*', str(agent))[-1][0] + 1):
    #         x = (i, agent, ask_block.select(i, '*', '*', str(agent))[0])