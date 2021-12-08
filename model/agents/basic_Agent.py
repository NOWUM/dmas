# third party modules
import time

import pandas as pd
import pika
import logging
import os
from sqlalchemy import create_engine

from interfaces.infrastructure import InfrastructureInterface


class BasicAgent:

    def __init__(self, date, plz, typ, connect, infrastructure_source, infrastructure_login, *args, **kwargs):

        # declare meta data for each agent
        self.plz = plz                                              # plz code
        self.typ = typ                                              # agent type
        self.name = f'{self.typ}_{self.plz}'                        # name
        self.date = pd.to_datetime(date)                            # current day
        self.exchange_name = 'dMas'

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

        self.simulation_database = create_engine(f'postgresql://dMAS:dMAS@simulationdb/dMAS',
                                                 connect_args={"application_name": self.name})
        self.infrastructure_interface = InfrastructureInterface(infrastructure_source, infrastructure_login)
        self.longitude, self.latitude = self.infrastructure_interface.get_position(plz)

        self.channels = []
        if connect:
            self.channel = self.get_rabbitmq_connection()
            result = self.channel.queue_declare(queue=self.name, exclusive=True)
            self.channel.queue_bind(exchange=self.exchange_name, queue=result.method.queue)

    def __del__(self):
        for connection, channel in self.channels:
            if connection and connection.is_closed:
                connection.close()

    def get_rabbitmq_connection(self):
        for i in range(5):
            try:
                mqtt_connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq', heartbeat=0))
                channel = mqtt_connection.channel()
                channel.exchange_declare(exchange=self.exchange_name, exchange_type='fanout')
                self.logger.info(f'connected to rabbitmq')
                self.channels.append((mqtt_connection, channel))
                return channel
            except pika.exceptions.AMQPConnectionError:
                    self.logger.info(f'could not connect - try: {i}')
                    time.sleep(i ** 2)
            except Exception as e:
                    self.mqtt_connection = False
                    self.logger.exception(f'could not connect - try: {i}')
        return False, None

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