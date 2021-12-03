# third party modules
import time as tme
import pandas as pd
import numpy as np
import os

# model modules
from agents.basic_Agent import BasicAgent

class CtlAgent(BasicAgent):

    def __init__(self, date, plz, agent_type, mqtt_exchange, connect,  infrastructure_source, infrastructure_login):
        super().__init__(date, plz, agent_type, mqtt_exchange, connect, infrastructure_source, infrastructure_login)
        self.logger.info('starting the agent')
        start_time = tme.time()
        self.logger.info('setup of the agent completed in %s' % (tme.time() - start_time))

    def run(self):

        for date in pd.date_range(start=self.start, end=self.end, freq='D'):

            try:
                start_time = tme.time()  # timestamp to measure simulation time

                # 1.Step: Run optimization for dayAhead Market
                self.channel.basic_publish(exchange=self.exchange, routing_key='',
                                           body='opt_dayAhead ' + str(date))
                # 2. Step: Run Market Clearing
                self.channel.basic_publish(exchange=self.exchange, routing_key='',
                                           body='dayAhead_clearing ' + str(date))

                # TODO: Changes for wtr Agent
                # weather_generator.generate_weather(valid=valid, date=pd.to_datetime(start))
                # while not m_con.get_market_status(date):  # check if clearing done
                #     if gen_weather:
                #         weather_generator.generate_weather(valid, date + pd.DateOffset(days=1))
                #         gen_weather = False
                #     else:
                #         tme.sleep(1)
                gen_weather = True

                # 3. Step: Run Power Flow calculation
                self.channel.basic_publish(exchange=self.exchange, routing_key='',
                                           body='grid_calc ' + str(date))
                # 4. Step: Publish Market Results
                self.channel.basic_publish(exchange=self.exchange, routing_key='',
                                           body='result_dayAhead ' + str(date))

                end_time = tme.time() - start_time
                self.logger.info('Day %s complete in: %s seconds ' % (str(date.date()), end_time))

            except Exception as e:
                print('Error ' + str(date.date()))
                print(e)

