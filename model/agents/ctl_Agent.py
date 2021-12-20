# third party modules
import time
import pandas as pd
from flask import Flask, request, redirect
import threading
import requests
import numpy as np
from tqdm import tqdm
import dash
from dash import dcc
from dash import html
import plotly.express as px
from dash.dependencies import Input, Output

# model modules
from agents.basic_Agent import BasicAgent

# app = Flask('dMAS_controller')
app = dash.Dash('dMAS_controller')


class CtlAgent(BasicAgent):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger.info('starting the agent')
        start_time = time.time()
        self.sim_start = False
        self.sim_stop = False

        self.start_date = pd.to_datetime('2018-01-01')
        self.stop_date = pd.to_datetime('2018-02-01')
        self.waiting_list = []
        self.cleared = False

        self.logger.info(f'setup of the agent completed in {np.round(time.time() - start_time,2)} seconds')

    def set_agents(self):
        headers = {'content-type': 'application/json', }
        response = requests.get(f'http://{self.mqtt_server}:15672/api/queues', headers=headers, auth=('guest', 'guest'))
        agents = response.json()
        for agent in agents:
            name = agent['name']
            if name[:3] in ['dem', 'res', 'str', 'pwp']:
                self.waiting_list.append(name)
        self.logger.info(f'{len(self.waiting_list)} agent(s) are running')

    def get_agents(self):
        headers = {'content-type': 'application/json', }
        response = requests.get(f'http://{self.mqtt_server}:15672/api/queues', headers=headers, auth=('guest', 'guest'))
        agents = response.json()
        return [agent['name'] for agent in agents if agent['name'][:3]
                in ['dem', 'res', 'str', 'pwp']]

    def wait_for_agents(self):
        t = time.time()
        total = len(self.waiting_list)
        still_waiting = len(self.waiting_list)
        with tqdm(total=total) as pbar:
            while len(self.waiting_list) > 0:
                if time.time() - t > 30:
                    current_agents = self.get_agents()
                    for agent in self.waiting_list:
                        if agent not in current_agents:
                            self.waiting_list.remove(agent)
                            self.logger.info(f'{agent} left the simulation stack')
                            self.logger.info(f'removed agent {agent} from current list')
                        else:
                            self.logger.info(f'still waiting for {agent}')
                if time.time() - t > 120:
                    for agent in self.waiting_list:
                        self.waiting_list.remove(agent)
                        self.logger.info(f'get no response of {agent}')
                        self.logger.info(f'removed agent {agent} from current list')
                pbar.update(still_waiting - len(self.waiting_list))
                still_waiting = len(self.waiting_list)

                time.sleep(1)

    def wait_for_market(self):
        t = time.time()
        while not self.cleared:
            self.logger.info(f'still waiting for market clearing')
            time.sleep(1)
            if time.time() - t > 120:
                self.cleared = True
        self.cleared = False

    def callback(self, ch, method, properties, body):
        super().callback(ch, method, properties, body)
        message = body.decode("utf-8")
        agent, date = message.split(' ')
        date = pd.to_datetime(date)
        # print(message)
        if date == self.date and agent in self.waiting_list:
            # self.logger.info(f'Agent {agent} set orders')
            self.waiting_list.remove(agent)
        if agent == 'MRK_1' and date == self.date:
            self.cleared = True

    def check_orders(self):
        try:
            self.channel.basic_consume(queue=self.name, on_message_callback=self.callback, auto_ack=True)
            print(' --> Waiting for orders')
            self.channel.start_consuming()
        except Exception as e:
            print(repr(e))

    def simulation_routine(self):
        self.logger.info('simulation started')

        for date in tqdm(pd.date_range(start=self.start_date, end=self.stop_date, freq='D')):
            if self.sim_stop:
                self.logger.info('simulation terminated')
                break
            else:
                try:
                    start_time = time.time()

                    self.date = date.date()

                    # 1.Step: optimization for dayAhead Market
                    self.set_agents()
                    self.publish.basic_publish(exchange=self.mqtt_exchange, routing_key='',
                                               body=f'opt_dayAhead {date.date()}')

                    self.wait_for_agents()
                    self.logger.info('agents set their orders')

                    # 2. Step: run market clearing
                    self.publish.basic_publish(exchange=self.mqtt_exchange, routing_key='',
                                               body=f'dayAhead_clearing {date.date()}')

                    self.wait_for_market()
                    self.logger.info(f'day ahead clearing finished')

                    # 3. Step: agents have to adjust their demand and generation
                    self.publish.basic_publish(exchange=self.mqtt_exchange, routing_key='',
                                               body=f'result_dayAhead {date.date()}')

                    # 4. Step: reset the order_book table for the next day
                    self.simulation_interface.reset_order_book()
                    self.logger.info(f'finished day in {np.round(time.time() - start_time, 2)} seconds')

                    self.publish.basic_publish(exchange=self.exchange_name, routing_key='',
                                               body=f'set_capacities {self.start_date.date()}')

                except Exception as e:
                    print(repr(e))
                    self.logger.exception('Error in Simulation')

        self.sim_start = False
        self.logger.info('simulation finished')

    def run(self):

        self.simulation_interface.initial_tables()
        self.logger.info('initialize database for simulation')

        if not self.sim_start:
            content = html.Form(children=[
                                    html.Span(children=[
                                        html.Label('Start Date', htmlFor="start_date"),
                                        html.Br(),
                                        dcc.Input(type="date", id="start_date", name="start_date", value="1995-01-01",
                                                  style={'display': 'flex', 'flex-direction': 'column'})
                                    ], style={'margin-bottom': '20px'}),
                                    html.Span(children=[
                                        html.Label('End Date', htmlFor="end_date"),
                                        html.Br(),
                                        dcc.Input(type="date", id="end_date", name="end_date", value="1995-02-01",
                                                  style={'display': 'flex', 'flex-direction': 'column'})
                                    ], style={'margin-bottom': '20px'}),
                                    dcc.Input(type="submit", value="Start Simulation", id='start_button')
                                ], method="POST", action="/start")
        else:
            content = html.Form(children=[
                                    dcc.Input(type="submit", value="Start Simulation")
                                ], method="POST", action="/start")

        app.layout = html.Div(children=[html.H1('Docker Agent-based Simulation'),
                                        html.P(f'Simulation running: {self.sim_start}'),
                                        content
                               ], style={'width': '60%', 'margin': 'auto', 'height': '80%'})

        @app.server.route('/stop')
        def stop_simulation():
            self.sim_stop = True
            self.logger.info('stopping simulation')
            return redirect('/')

        @app.server.route('/start', methods=['POST'])
        def run_simulation():
            rf = request.form
            self.start_date = pd.to_datetime(rf.get('start', '1995-01-01'))
            self.stop_date = pd.to_datetime(rf.get('stop', '1995-02-01'))

            if not self.sim_start:
                simulation = threading.Thread(target=self.simulation_routine)
                check_orders = threading.Thread(target=self.check_orders)
                simulation.start()
                check_orders.start()
                self.sim_start = True

            return redirect('/')

        app.run_server(debug=False, port=5000, host='0.0.0.0')

