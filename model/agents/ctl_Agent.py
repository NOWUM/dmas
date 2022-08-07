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
from enum import Enum

# model modules
from agents.basic_Agent import BasicAgent

server = Flask('dMAS_controller')

class SimStep(Enum):
    READY = -1
    MARKET_BIDS = 0
    MARKET_CLEARING = 1
    RESULTS = 2
    CAPACITIES = 3

class CtlAgent(BasicAgent):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        start_time = time.time()

        self.start_date = pd.to_datetime('2018-01-01')
        self.stop_date = pd.to_datetime('2018-02-01')

        self.registered_agents = dict()
        self.waiting_list = []
        self.wait_limit = 100

        self.cleared = False
        self.simulation_interface.date = self.start_date

        self.ws_uri = f"{kwargs['ws_host']}:{kwargs['ws_port']}"
        self.loop = asyncio.get_event_loop()

        self.simulation_step = SimStep.READY
        self.sim_connector = threading.Thread(target=self.start_sim_server)
        self.sim_connector.start()
        self.logger.info(f'setup of the agent completed in {time.time() - start_time:.2f} seconds')


    async def async_run(self):
        host, port = self.ws_uri.split(':')
        server = await websockets.serve(self.handler, host, int(port))
        self.logger.info('finished serving')
        await server.wait_closed()

    def run(self):
        # overwritten without parent call
        # to suppress connecting as client
        self.loop.run_until_complete(self.async_run())

    async def receive_message(self, ws):
        async for message in ws:
            name = message.split(' ')[-1]

            if 'optimized_dayAhead' in message:
                if name in self.waiting_list:
                    self.waiting_list.remove(name)
                else:
                    self.logger.error(f'controller went on without waiting for {name}')
                    self.logger.error(f'the wait_limit should be increased')
                self.logger.info(f'agent {name} set orders')
            if 'cleared market' in message:
                self.cleared = True
                self.logger.info('market has cleared the market')

    async def send_message(self):
        duration_in_state = 0
        previous_state = self.simulation_step
        while self.date != self.stop_date:
            if previous_state != self.simulation_step:
                duration_in_state = 0
                previous_state = self.simulation_step
            duration_in_state +=1

            connected = set(self.registered_agents.values())
            # -> 1.Step: optimize day Ahead
            if self.simulation_step == SimStep.MARKET_BIDS:
                # initialize waiting list to wait for market clearing
                for agent_name in self.registered_agents.keys():
                    if (agent_name != 'market' and 
                        'net' not in agent_name):
                        self.waiting_list.append(agent_name)

                websockets.broadcast(connected, f"optimize_dayAhead {self.date.date()}")
                self.logger.info(f'send command: optimize_dayAhead {self.date.date()}')
                self.simulation_step = SimStep.MARKET_CLEARING
            # -> 2.Step: clear market
            elif self.simulation_step == SimStep.MARKET_CLEARING:
                if len(self.waiting_list) <= 15 and duration_in_state%15==0:
                    self.logger.info(self.waiting_list)
                elif len(self.waiting_list) == 0 or duration_in_state > self.wait_limit:
                    if len(self.waiting_list) >0:
                        self.logger.info(f'aborted waiting list {self.waiting_list}')
                        self.waiting_list.clear()

                    if 'market' in self.registered_agents.keys():
                        await self.registered_agents['market'].send(f'clear_market {self.date.date()}')
                        self.logger.info('send command: clear_market')
                    else:
                        self.logger.info('no market found')
                    self.simulation_step = SimStep.RESULTS
            # -> 3.Step: adjust power to market results
            elif self.cleared and self.simulation_step == SimStep.RESULTS:
                websockets.broadcast(connected, f"results_dayAhead {self.date.date()}")
                self.logger.info(f'send command: results_dayAhead {self.date.date()}')
                self.simulation_step = SimStep.CAPACITIES
            # -> 4.Step: store current capacities
            elif self.simulation_step == SimStep.CAPACITIES:
                websockets.broadcast(connected, f"set_capacities {self.date.date()}")
                self.logger.info('send command: set_capacities')
                # -> prepare next day
                self.simulation_step = SimStep.MARKET_BIDS
                self.cleared = False
                self.date += timedelta(days=1)
            await asyncio.sleep(1)
        self.logger.info('finished simulation')
        websockets.broadcast(connected, f"finished {self.date.date()}")

    async def handler(self, ws):
        '''
        handles new websocket connections
        - registers clients
        - dispatches messages
        '''

        agent_name = ws.path.strip('/')
        self.registered_agents[agent_name] = ws
        self.logger.info(f'{agent_name} connects')
        try:
            # messages are broadcasted in market
            # other websocket only need to handle received messages
            # this reduces races in logging while waiting
            if agent_name == 'market':
                await asyncio.gather(self.receive_message(ws), self.send_message())
            else:
                await self.receive_message(ws)
        except websockets.exceptions.ConnectionClosed:
            self.logger.info(f"Agent {agent_name} closed connection")
        except Exception:
            self.logger.exception('Error in Agent Handler')
        finally:
            self.logger.info(f"Agent {agent_name} disconnected")
            if agent_name in self.waiting_list:
                self.waiting_list.remove(agent_name)
            del self.registered_agents[agent_name]

    def start_sim_server(self):

        @server.route('/start', methods=['POST'])
        def start_post():
            if self.simulation_step == SimStep.READY:
                self.start_date = pd.to_datetime(request.form.get('begin'))
                self.date = self.start_date
                self.stop_date = pd.to_datetime(request.form.get('end'))
                self.simulation_step = SimStep.MARKET_BIDS
                return 'OK', 200
            else:
                return 'Simulation already running', 409

        @server.route('/agent_count')
        def agent_count():
            return str(len(self.registered_agents)), 200

        @server.route('/wait_limit', methods=['POST'])
        def wait_duration_limit():
            self.wait_limit = int(request.form.get('wait_limit', 100))
            return f'set wait_limit to {self.wait_limit}', 200

        server.run(debug=False, port=5000, host='0.0.0.0')