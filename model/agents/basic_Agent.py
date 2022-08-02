# third party modules
import pandas as pd
import logging
import time
import asyncio
import websockets
from websockets import WebSocketClientProtocol as wsClientPrtl

from interfaces.weather import WeatherInterface
from interfaces.structure import InfrastructureInterface, get_lon_lat
from interfaces.simulation import SimulationInterface
from functools import partial

class BasicAgent:

    def __init__(self, area, type, date, *args, **kwargs):

        # declare meta data
        self.area = area
        self.type = type
        self.name = f'{self.type}_{self.area}'.lower() if 'mrk' not in type.lower() else 'market'
        self.date = pd.to_datetime(date)

        # declare logging options
        self.logger = logging.getLogger(self.name)
        self.logger.setLevel(logging.INFO)

        # declare websocket parameter
        self.ws_uri = f"ws://{kwargs['ws_host']}:{kwargs['ws_port']}/{self.name}"
        self.running = False
        self.connection_tries = 3

        # declare simulation data server
        self.simulation_data_server = kwargs['simulation_server']
        self.simulation_data_credential = kwargs['simulation_credential']
        self.simulation_database = kwargs['simulation_database']
        self.simulation_interface = SimulationInterface(self.name, self.simulation_data_server,
                                                        self.simulation_data_credential,
                                                        self.simulation_database,
                                                        kwargs['ws_host'])
        # declare structure data sever
        self.structure_data_server = kwargs['structure_server']
        self.structure_data_credential = kwargs['structure_credential']
        self.infrastructure_interface = InfrastructureInterface(self.name, self.structure_data_server,
                                                                self.structure_data_credential)
        self.longitude, self.latitude = get_lon_lat(self.area)

        # declare weather data server
        self.weather_interface = WeatherInterface(self.name, kwargs['weather_database_uri'])

        self.logger.info('starting the agent')

    def __del__(self):
        self.logger.info('shutting down')

    async def message_handler(self, ws: wsClientPrtl):
        self.running = True
        self.logger.info(f' --> Agent {self.name} has connected to simulation '
                  f'and is waiting for instructions')

        while self.running:
            async for message in ws:
                message, date = message.split(' ')
                self.date = pd.to_datetime(date)

                if 'finished' in message:
                    self.running = False
                loop = asyncio.get_event_loop()
                function = partial(self.handle_message, message=message)
                msg = await loop.run_in_executor(None, function)
                if msg:
                    await ws.send(msg)
            await asyncio.sleep(0.1)
    
    def handle_message(self, message):
        '''
        empty message handler - overwritten in implementation
        
        returns response message
        '''
        pass


    async def connect(self):
        i = 0
        while i < self.connection_tries:
            try:
                await asyncio.sleep(2*i)
                self.logger.info(f'connecting to {self.ws_uri}')
                # wait at the beginning
                async with websockets.connect(self.ws_uri) as ws:
                    await self.message_handler(ws)
            except OSError:
                i += 1
                self.logger.error('could not connect to '+self.ws_uri)
            except websockets.exceptions.ConnectionClosed as e:
                i += 1
                self.logger.error(f"Controller was shut down, trying again {i}")

    def run(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.connect())


if __name__ == '__main__':
    pass
