# third party modules
import pandas as pd
import pika
import logging
import os
from sqlalchemy import create_engine

from interfaces.infrastructure import InfrastructureInterface


class BasicAgent:

    def __init__(self, date, plz, typ, mqtt_exchange, connect, infrastructure_source, infrastructure_login):

        # declare meta data for each agent
        self.plz = plz                                              # plz code
        self.typ = typ                                              # agent type
        self.name = f'{self.typ}_{self.plz}'                        # name
        self.date = pd.to_datetime(date)                            # current day

        # declare logging options
        self.logger = logging.getLogger(self.name)
        self.logger.setLevel(logging.INFO)
        if not os.path.exists(r'./logs'):
            os.mkdir(r'./logs')
        fh = logging.FileHandler(r'./logs/%s.log' % self.name)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)
        # self.logger.disabled = True

        # dictionary for performance measuring
        self.performance = dict(initModel=0,                        # build model for da optimization
                                optModel=0,                         # optimize for da market
                                saveSchedule=0,                     # save optimization results in influx db
                                buildOrders=0,                      # construct order book
                                sendOrders=0,                       # send orders to mongodb
                                adjustResult=0,                     # adjustments corresponding to da results
                                saveResult=0,                       # save adjustments in influx db
                                nextDay=0)                          # preparation for coming day

        self.simulation_database = create_engine(f'postgresql://dMas:dMas@simulationdb/dMAS',
                                                 connect_args={"application_name": self.name})

        self.exchange = mqtt_exchange
        self.mqtt_connection = False
        if connect:
            try:
                self.mqtt_connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq', heartbeat=0))
                self.channel = self.mqtt_connection.channel()
                self.channel.exchange_declare(exchange=self.exchange, exchange_type='fanout')
                result = self.channel.queue_declare(queue=self.name, exclusive=True)
                self.channel.queue_bind(exchange=self.exchange, queue=result.method.queue)
            except Exception as e:
                self.mqtt_connection = False
                self.logger.exception('Cant connect to MQTT')
        try:
            self.infrastructure_interface = InfrastructureInterface(infrastructure_source, infrastructure_login)
        except Exception as e:
            self.logger.exception('Cant connect to Infrastructure Database')

    def __del__(self):
        if self.mqtt_connection and not self.mqtt_connection.is_closed:
            self.mqtt_connection.close()

    def callback(self, ch, method, properties, body):
        message = body.decode("utf-8")
        self.date = pd.to_datetime(message.split(' ')[1])

        # Terminate Agent
        # -----------------------------------------------------------------------------------------------------------
        if 'kill' in message or self.name in message or self.typ + '_all' in message:
            if not self.mqtt_connection.is_closed:
                self.mqtt_connection.close()

    def run(self):
        if self.mqtt_connection:
            self.channel.basic_consume(queue=self.name, on_message_callback=self.callback, auto_ack=True)
            print(' --> Agent %s has connected to the marketplace, waiting for instructions (to exit press CTRL+C)'
                  % self.name)
            self.channel.start_consuming()


if __name__ == '__main__':

    agent = BasicAgent(plz=3, date=pd.to_datetime(2021-11-19))
    agent.run()