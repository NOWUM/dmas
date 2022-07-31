# third party modules
import time
import pandas as pd
from datetime import timedelta
from flask import Flask, request, redirect
import threading
import numpy as np
from tqdm import tqdm
import websockets
import asyncio

# model modules
from agents.basic_Agent import BasicAgent

server = Flask('dMAS_controller')


class CtlAgent(BasicAgent):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        start_time = time.time()

        self.start_date = pd.to_datetime('2018-01-01')
        self.stop_date = pd.to_datetime('2018-02-01')

        self.registered_agents = dict()
        self.waiting_list = []

        self.cleared = False
        self.simulation_interface.date = self.start_date

        self.ws_uri = f"{kwargs['ws_host']}:{kwargs['ws_port']}"
        self.loop = asyncio.get_event_loop()

        self.simulation_step = -1
        self.total_agents = 0
        self.sim_connector = threading.Thread(target=self.start_simulation)
        self.sim_connector.start()
        self.logger.info(f'setup of the agent completed in {time.time() - start_time:.2f} seconds')

    def run(self):
        host, port = self.ws_uri.split(':')
        server = websockets.serve(self.handler, host, int(port))
        self.loop.run_until_complete(server)
        self.loop.run_forever()

    async def receive_message(self, ws):
        async for message in ws:
            name = message.split(' ')[-1]
            if 'register' in message:  # -> register agent
                self.registered_agents[name] = ws
                self.waiting_list.append(name)
                self.logger.info(f'{name} connects')
            if 'optimized_dayAhead' in message:
                self.waiting_list.remove(name)
                self.logger.info(f'agent {name} set orders')
            if 'cleared market' in message:
                self.cleared = True
                self.logger.info('market has cleared the market')

    async def send_message(self):
        while True:
            connected = set(self.registered_agents.values())
            # -> 1.Step: optimize day Ahead
            if len(self.registered_agents) == self.total_agents and self.simulation_step == 0:
                websockets.broadcast(connected, "optimize_dayAhead")
                self.logger.info('send command: optimize_dayAhead')
                self.simulation_step += 1
            # -> 2.Step: clear market
            elif len(self.registered_agents) == 0 and self.simulation_step == 1:
                await self.registered_agents['market'].send('clear_market')
                self.logger.info('send command: clear_market')
                self.simulation_step += 1
            # -> 3.Step: adjust power to market results
            elif self.cleared and self.simulation_step == 2:
                websockets.broadcast(connected, "results_dayAhead")
                self.logger.info('send command: results_dayAhead')
                self.simulation_step += 1
            # -> 4.Step: store current capacities
            elif self.simulation_step == 3:
                websockets.broadcast(connected, "set_capacities")
                self.logger.info('send command: set_capacities')
                # -> prepare next day
                self.simulation_step = 0
                self.cleared = False
                self.waiting_list = [*self.registered_agents.keys()]
                self.date += timedelta(days=1)
            # -> terminate simulation
            if self.date == self.stop_date:
                websockets.broadcast(connected, "finished")
                self.registered_agents = dict()
            await asyncio.sleep(2.5)

    async def handler(self, ws):
        await asyncio.gather(self.receive_message(ws), self.send_message())


    def start_simulation(self):

        @server.route('/start', methods=['POST'])
        def start_post():
            self.start_date = pd.to_datetime(request.form.get('begin'))
            self.stop_date = pd.to_datetime(request.form.get('end'))
            self.simulation_step = 0

        server.run(debug=False, port=5005, host='0.0.0.0')