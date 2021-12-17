# third party modules
import time
import pandas as pd
import pika
import logging
import os
from sqlalchemy import create_engine

from interfaces.infrastructure import InfrastructureInterface


class BasicAgent:

    def __init__(self, date, plz, type, connect, infrastructure_source, infrastructure_login,
                 mqtt_host, mqtt_exchange, simulation_database, simulation_login, *args, **kwargs):

        # declare meta data for each agent
        self.plz = plz
        self.typ = type
        self.name = f'dmas_{self.typ}{self.plz}'.lower()
        self.date = pd.to_datetime(date)

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
        self.simulation_database = create_engine(f'postgresql://{simulation_login}@{simulation_database}/dMAS',
                                                   connect_args={"application_name": self.name})
        self.infrastructure_interface = InfrastructureInterface(infrastructure_source, infrastructure_login)
        self.mqtt_host = mqtt_host
        self.exchange_name = mqtt_exchange
        self.longitude, self.latitude = self.infrastructure_interface.get_position(plz)

        self.channels = []
        if connect:
            self.publish = self.get_rabbitmq_connection()
            self.channel = self.get_rabbitmq_connection()
            result = self.channel.queue_declare(queue=self.name, exclusive=True)
            self.channel.queue_bind(exchange=self.exchange_name, queue=result.method.queue)

    def __del__(self):
        for connection, channel in self.channels:
            if connection and not connection.is_closed:
                connection.close()

    def get_rabbitmq_connection(self):
        mqtt_connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.mqtt_host, heartbeat=0))
        channel = mqtt_connection.channel()
        channel.exchange_declare(exchange=self.exchange_name, exchange_type='fanout')
        self.logger.info(f'connected to rabbitmq')
        self.channels.append((mqtt_connection, channel))
        return channel

        # for i in range(15):
        #     try:
        #         mqtt_connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq', heartbeat=0))
        #         channel = mqtt_connection.channel()
        #         channel.exchange_declare(exchange=self.exchange_name, exchange_type='fanout')
        #         self.logger.info(f'connected to rabbitmq')
        #         self.channels.append((mqtt_connection, channel))
        #         return channel
        #     except pika.exceptions.AMQPConnectionError:
        #             self.logger.info(f'could not connect - try: {i}')
        #             time.sleep(i ** 2)
        #     except Exception as e:
        #             self.mqtt_connection = False
        #             self.logger.exception(f'could not connect - try: {i}')
        # return False, None

    def callback(self, ch, method, properties, body):
        message = body.decode("utf-8")
        self.date = pd.to_datetime(message.split(' ')[1])
        # print(message)

    def run(self):
        self.channel.basic_consume(queue=self.name, on_message_callback=self.callback, auto_ack=True)
        print(' --> Agent %s has connected to the marketplace, waiting for instructions (to exit press CTRL+C)'
              % self.name)
        self.channel.start_consuming()
