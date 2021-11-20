# third party modules
import time as tme
import pandas as pd
import numpy as np

# model modules
from apps.market import market
from agents.basic_Agent import BasicAgent


class MarketAgent(BasicAgent):

    def __init__(self, date, plz):
        super().__init__(date=date, plz=plz, typ='MRK')
        self.logger.info('starting the agent')
        start_time = tme.time()
        self.market = market()
        self.logger.info('setup of the agent completed in %s' % (tme.time() - start_time))

    def get_orders(self, name, date):
        ask_orders = {}                                                     # all orders (ask)
        bid_orders = {}                                                     # all orders (bid)
        wait = True                                                         # check if orders from agent are delivered
        start = tme.time()                                                  # start waiting and collect orders
        while wait:
            x = self.connections['mongoDB'].orderDB[date].find_one({"_id": name})
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
                print('waiting for %s' % name)
                tme.sleep(1)                                                # wait a second and ask mongodb again
            end = tme.time()                                                # current timestamp
            if end - start >= 120:                                          # wait maximal 120 seconds
                print('get no orders of Agent %s' % name)
                wait = False

        orders = (ask_orders, bid_orders)
        return orders

    def clearing(self):

        agent_ids = self.connections['mongoDB'].get_agents(sorted=False)
        total_orders = [self.get_orders(agent, str(self.date.date())) for agent in agent_ids
                        if 'MRK' not in agent and 'NET' not in agent]

        for order in total_orders:
            self.market.set_parameter(ask=order[0], bid=order[1])

        result = self.market.optimize()

        # save result in influxdb
        time = self.date
        for element in result:
            # save all asks
            ask = pd.DataFrame.from_dict(element[0])
            ask.columns = ['power']
            ask['names'] = [name.split('-')[0] for name in ask.index]
            ask = ask.groupby('names').sum()
            ask['order'] = ['ask' for _ in range(len(ask))]
            ask['typ'] = [name.split('_')[0] for name in ask.index]
            ask['names'] = [name for name in ask.index]
            ask['area'] = [name.split('_')[1] for name in ask.index]
            ask.index = [time for _ in range(len(ask))]
            self.connections['influxDB'].influx.write_points(dataframe=ask, measurement='DayAhead', tag_columns=['names', 'order',
                                                                                               'typ', 'area'])
            # save all bids
            bid = pd.DataFrame.from_dict(element[1])
            bid.columns = ['power']
            bid['names'] = [name for name in bid.index]
            bid['order'] = ['bid' for _ in range(len(bid))]
            bid['typ'] = [name.split('_')[0] for name in bid['names'].to_numpy()]
            bid['area'] = [name.split('_')[1] for name in bid.index]
            bid.index = [time for _ in range(len(bid))]
            self.connections['influxDB'].influx.write_points(dataframe=bid, measurement='DayAhead', tag_columns=['names', 'order',
                                                                                               'typ', 'area'])
            # save mcp
            mcp = pd.DataFrame(data=[np.asarray(element[2], dtype=float)], index=[time], columns=['price'])
            self.connections['influxDB'].influx.write_points(dataframe=mcp, measurement='DayAhead')
            # next hour
            time += pd.DateOffset(hours=1)

        self.connections['mongoDB'].set_market_status(name='market', date=self.date)

        self.market.reset_parameter()