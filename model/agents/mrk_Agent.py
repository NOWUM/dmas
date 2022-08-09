# third party modules
import time as time
import pandas as pd
import numpy as np
from websockets import WebSocketClientProtocol as wsClientPrtl

# model modules
from systems.market import DayAheadMarket
from agents.basic_Agent import BasicAgent


class MarketAgent(BasicAgent):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        start_time = time.time()
        self.market = DayAheadMarket()

        self.logger.info(f'setup of the agent completed in {time.time() - start_time:.2f} seconds')

    def handle_message(self, message):
        if 'clear_market' in message:
            self.logger.info(f'started market clearing {self.date}')
            self.market_clearing()
            return f'cleared market {self.name}'
        elif 'set_capacities' in message:
            # now its time to reset the order book
            self.simulation_interface.reset_order_book()

    def market_clearing(self):
        start_time = time.time()
        df = self.simulation_interface.get_hourly_orders()
        ask = df.loc[df['type'] == 'generation']
        hourly_ask = {}
        for key, value in ask.to_dict(orient='index').items():
            hourly_ask.update({key: (value['price'], value['volume'])})

        hourly_bid = {}
        bid = df.loc[df['type'] == 'demand']
        for key, value in bid.to_dict(orient='index').items():
            hourly_bid.update({key: (value['price'], value['volume'])})
        self.logger.info(f'got hourly_orders in {time.time() - start_time:.2f} seconds')
        start_time = time.time()

        df = self.simulation_interface.get_linked_orders()
        linked_orders = {}
        for key, value in df.to_dict(orient='index').items():
            linked_orders.update({key: (value['price'], value['volume'], value['link'])})
        self.logger.info(f'got linked_orders in {time.time() - start_time:.2f} seconds')
        start_time = time.time()

        df = self.simulation_interface.get_exclusive_orders()
        exclusive_orders = {}
        for key, value in df.to_dict(orient='index').items():
            exclusive_orders.update({key: (value['price'], value['volume'])})
        self.logger.info(f'got exclusive_orders in {time.time() - start_time:.2f} seconds')
        start_time = time.time()

        self.market.set_parameter(hourly_ask, hourly_bid, linked_orders, exclusive_orders)
        self.logger.info(f'start market optimization')
        auction_results, used_ask_orders, used_linked_orders, used_exclusive_orders, used_bid_orders = self.market.optimize()
        self.logger.info('get market results')
        t1 = time.time()
        market_results = dict(
            hourly_results=used_ask_orders,
            linked_results=used_linked_orders,
            exclusive_results=used_exclusive_orders,
            bid_results=used_bid_orders
        )

        auction_results.index = pd.date_range(start=self.date, periods=24, freq='h')
        auction_results.index.name = 'time'
        self.logger.info(f'saved results in db in {round(time.time()-t1, 2)}')
        self.simulation_interface.set_auction_results(auction_results)
        self.simulation_interface.set_market_results(market_results)
        self.logger.info(f'successfully cleared market in {time.time() - start_time:.2f} seconds for {self.date}')