# third party modules
import time as time
import pandas as pd
import numpy as np

# model modules
from systems.market_ import DayAheadMarket
from agents.basic_Agent import BasicAgent


class MarketAgent(BasicAgent):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        plz = kwargs['plz']
        self.logger.info('starting the agent')
        start_time = time.time()
        self.market = DayAheadMarket()

        self.logger.info(f'setup of the agent completed in {np.round(time.time() - start_time,2)} seconds')

    def callback(self, ch, method, properties, body):
        super().callback(ch, method, properties, body)

        message = body.decode("utf-8")
        self.date = pd.to_datetime(message.split(' ')[1])

        if 'dayAhead_clearing' in message:
            self.market_clearing()

    def market_clearing(self):
        df = pd.read_sql("Select * from order_book", self.simulation_database)
        df = df.set_index(['block_id', 'hour', 'order_id', 'name'])

        ask = df.loc[df['type'] == 'generation']
        ask_orders = {}
        for key, value in ask.to_dict(orient='index').items():
            ask_orders.update({key: (value['price'], value['volume'], value['link'])})

        bid_orders = {}
        bid = df.loc[df['type'] == 'demand']
        for key, value in bid.to_dict(orient='index').items():
            bid_orders.update({key: (value['price'], value['volume'], value['link'])})


        self.market.set_parameter(bid_orders, ask_orders)
        auction_results , market_results = self.market.optimize()

        auction_results.index = pd.date_range(start=self.date, periods=24, freq='h')
        auction_results.index.name = 'time'

        market_results.to_sql('market_results', self.simulation_database, if_exists='replace')
        auction_results.to_sql('auction_results', self.simulation_database, if_exists='append')
        self.logger.info('cleared market and saved result in database')

        self.publish.basic_publish(exchange=self.exchange_name, routing_key='', body=f'{self.name} {self.date.date()}')