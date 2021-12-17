# third party modules
import pandas as pd
import pika
import logging
from sqlalchemy import create_engine

from interfaces.infrastructure import InfrastructureInterface


class BasicAgent:

    def __init__(self, *args, **kwargs):

        # declare meta data
        self.plz = kwargs['plz']
        self.typ = kwargs['type']
        self.name = f'{self.typ}{self.plz}'.lower()
        self.date = pd.to_datetime(kwargs['date'])
        # declare mqtt parameter
        self.mqtt_server = kwargs['mqtt_server']
        self.mqtt_exchange = kwargs['mqtt_exchange']
        # declare simulation data server
        self.simulation_engine = None
        self.simulation_data_server = kwargs['simulation_server']
        self.simulation_database = kwargs['simulation_database']
        # declare structure data sever
        self.structure_data_server = kwargs['structure_server']
        self.structure_data_credential= kwargs['structure_credential']
        self.infrastructure_interface = InfrastructureInterface(self.structure_data_server,
                                                                self.structure_data_credential)
        self.longitude, self.latitude = self.infrastructure_interface.get_position(self.plz)
        # declare logging options
        self.logger = logging.getLogger(self.name)
        self.logger.setLevel(logging.INFO)

        # declare mqtt options
        self.channels = []
        self.publish = self.get_rabbitmq_connection()
        self.channel = self.get_rabbitmq_connection()
        result = self.channel.queue_declare(queue=self.name, exclusive=True)
        self.channel.queue_bind(exchange=self.mqtt_exchange, queue=result.method.queue)

    def __del__(self):
        for connection, channel in self.channels:
            if connection and not connection.is_closed:
                connection.close()

    def get_simulation_data_connection(self):
        simulation_engine = create_engine(f'postgresql://dMAS:dMAS@{self.simulation_data_server}/'
                                          f'{self.simulation_database}', connect_args={"application_name": self.name})
        self.logger.info(f'connected to simulation database')
        return simulation_engine

    def get_rabbitmq_connection(self):
        mqtt_connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.mqtt_server, heartbeat=0))
        channel = mqtt_connection.channel()
        channel.exchange_declare(exchange=self.mqtt_exchange, exchange_type='fanout')
        self.logger.info(f'connected to rabbitmq')
        self.channels.append((mqtt_connection, channel))
        return channel

    def callback(self, ch, method, properties, body):
        message = body.decode("utf-8")
        self.date = pd.to_datetime(message.split(' ')[1])


    def run(self):
        self.channel.basic_consume(queue=self.name, on_message_callback=self.callback, auto_ack=True)
        print(f' --> Agent {self.name} has connected to simulation '
              f'and is waiting for instructions (to exit press CTRL+C)')
        self.channel.start_consuming()
