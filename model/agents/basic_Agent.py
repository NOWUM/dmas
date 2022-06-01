# third party modules
import pandas as pd
import pika
import logging
import time

from interfaces.weather import WeatherInterface
from interfaces.structure import InfrastructureInterface, get_lon_lat
from interfaces.simulation import SimulationInterface


class BasicAgent:

    def __init__(self, area, type, date, *args, **kwargs):

        # declare meta data
        self.area = area
        self.type = type
        self.name = f'{self.type}_{self.area}'.lower()
        self.date = pd.to_datetime(date)

        # declare logging options
        self.logger = logging.getLogger(self.name)
        self.logger.setLevel(logging.INFO)

        # declare mqtt parameter
        self.mqtt_server = kwargs['mqtt_server']
        self.mqtt_exchange = kwargs['mqtt_exchange']

        # declare simulation data server
        self.simulation_data_server = kwargs['simulation_server']
        self.simulation_data_credential = kwargs['simulation_credential']
        self.simulation_database = kwargs['simulation_database']
        self.simulation_interface = SimulationInterface(self.name, self.simulation_data_server,
                                                        self.simulation_data_credential,
                                                        self.simulation_database,
                                                        self.mqtt_server)
        # declare structure data sever
        self.structure_data_server = kwargs['structure_server']
        self.structure_data_credential= kwargs['structure_credential']
        self.infrastructure_interface = InfrastructureInterface(self.name, self.structure_data_server,
                                                                self.structure_data_credential)
        self.longitude, self.latitude = get_lon_lat(self.area)

        # declare weather data server
        self.weather_interface = WeatherInterface(self.name, kwargs['weather_database_uri'])

        # declare mqtt options
        self.channels = []
        self.publish = self.get_rabbitmq_connection()
        self.channel = self.get_rabbitmq_connection()
        # auto_delete deletes if consumer was connected and channel is then closed
        result = self.channel.queue_declare(queue=self.name, auto_delete=True)
        self.channel.queue_bind(exchange=self.mqtt_exchange, queue=result.method.queue)
        self.logger.info('starting the agent')

    def __del__(self):
        self.logger.info('shutting down')
        for connection, channel in self.channels:
            if connection and not connection.is_closed:
                connection.close()

    def get_rabbitmq_connection(self):
        for i in range(1, 11):
            try:
                mqtt_connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.mqtt_server, blocked_connection_timeout=3600, heartbeat=0))
                channel = mqtt_connection.channel()
                channel.exchange_declare(exchange=self.mqtt_exchange, exchange_type='fanout')
                self.logger.info(f'connected to rabbitmq')
                self.channels.append((mqtt_connection, channel))
                return channel
            except Exception as e:
                self.logger.info(f'connection failed - try: {i} - {repr(e)}')
                time.sleep(i ** 2)
        return None

    def callback(self, ch, method, properties, body):
        message = body.decode("utf-8")
        self.date = pd.to_datetime(message.split(' ')[1])
        self.logger.debug(f'get command {message}')
        return message


    def run(self):
        self.channel.basic_consume(queue=self.name, on_message_callback=self.callback, auto_ack=True)
        print(f' --> Agent {self.name} has connected to simulation '
              f'and is waiting for instructions (to exit press CTRL+C)')
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            print('KeyBoardInterrupt')


if __name__ == '__main__':
    mqtt_connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', blocked_connection_timeout=3600, heartbeat=0))

    channel = mqtt_connection.channel()
    channel.exchange_declare(exchange='dmas', exchange_type='fanout')
    result = channel.queue_declare(queue='test', exclusive=False, auto_delete=True)
    channel.queue_bind(exchange='dmas', queue=result.method.queue)

    #if mqtt_connection and not mqtt_connection.is_closed:
    #    mqtt_connection.close()