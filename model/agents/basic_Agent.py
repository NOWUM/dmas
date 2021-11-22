# third party modules
import pandas as pd
import pika
import logging
import os


class BasicAgent:

    def __init__(self, date, plz, typ, mqtt_exchange, simulation_database):
        os.getenv('USER', 'default_user')

        # declare meta data for each agent
        self.plz = plz                                              # plz code
        self.typ = typ                                              # agent type
        self.name = f'{self.typ}_{self.plz}'                        # name
        self.date = pd.to_datetime(date)                            # current day

        # dictionary for performance measuring
        self.performance = dict(initModel=0,                        # build model for da optimization
                                optModel=0,                         # optimize for da market
                                saveSchedule=0,                     # save optimization results in influx db
                                buildOrders=0,                      # construct order book
                                sendOrders=0,                       # send orders to mongodb
                                adjustResult=0,                     # adjustments corresponding to da results
                                saveResult=0,                       # save adjustments in influx db
                                nextDay=0)                          # preparation for coming day

        self.exchange = mqtt_exchange
        crd = pika.PlainCredentials('dMAS', 'dMAS')
        self.mqtt_connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', heartbeat=0))
                                                                                 # credentials=crd))
        self.channel = self.mqtt_connection.channel()
        self.channel.exchange_declare(exchange=self.exchange, exchange_type='fanout')
        result = self.channel.queue_declare(queue=self.name, exclusive=True)
        self.channel.queue_bind(exchange=self.exchange, queue=result.method.queue)

        self.database = simulation_database                          # name of simulation database

        # declare logging options
        self.logger = logging.getLogger(self.name)
        self.logger.setLevel(logging.INFO)
        fh = logging.FileHandler(r'./logs/%s.log' % self.name)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)
        # self.logger.disabled = True

    def __del__(self):
        if not self.mqtt_connection.is_closed:
            self.mqtt_connection.close()

    def callback(self, ch, method, properties, body):
        message = body.decode("utf-8")
        # print(message)
        self.date = pd.to_datetime(message.split(' ')[1])
        # Call DayAhead Optimization Methods for each Agent
        # -----------------------------------------------------------------------------------------------------------
        if 'opt_dayAhead' in message:
            try:
                if self.typ != 'NET' and self.typ != 'MRK':
                    self.optimize_dayAhead()
            except Exception as inst:
                self.exception_handle(part='Day Ahead Plan', inst=inst)

        # Call DayAhead Result Methods for each Agent
        # -----------------------------------------------------------------------------------------------------------
        if 'result_dayAhead' in message:
            try:
                if self.typ != 'NET' and self.typ != 'MRK':
                    self.post_dayAhead()
            except Exception as inst:
                self.exception_handle(part='Day Ahead Result', inst=inst)

        # Call for Power Flow Calculation
        # -----------------------------------------------------------------------------------------------------------
        if 'grid_calc' in message:
            try:
                if self.typ == 'NET':
                    self.calc_power_flow()
            except Exception as inst:
                self.exception_handle(part='Grid Calculation', inst=inst)

        # Call for Market Clearing
        # -----------------------------------------------------------------------------------------------------------
        if 'dayAhead_clearing' in message:
            try:
                if self.typ == 'MRK':
                    self.clearing()
            except Exception as inst:
                self.exception_handle(part='dayAhead Clearing', inst=inst)

        # Terminate Agents
        # -----------------------------------------------------------------------------------------------------------
        if 'kill' in message or self.name in message or self.typ + '_all' in message:
            if not self.mqtt_connection.is_closed:
                self.mqtt_connection.close()
            print('terminate', self.name)

    def run(self):

        self.channel.basic_consume(queue=self.name, on_message_callback=self.callback, auto_ack=True)
        print(' --> Agent %s has connected to the marketplace, waiting for instructions (to exit press CTRL+C)'
              % self.name)
        self.channel.start_consuming()


    def exception_handle(self, part, inst):
        print(self.name)
        print('Error in ' + part)
        print('Error --> ' + str(inst))



if __name__ == '__main__':

    agent = BasicAgent(plz=3, date=pd.to_datetime(2021-11-19))
    agent.run()
