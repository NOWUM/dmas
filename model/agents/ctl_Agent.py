# third party modules
import time
import pandas as pd
from flask import Flask, request, redirect
import threading
import numpy as np
from tqdm import tqdm
import dash
from dash.dependencies import Input, Output, State

# model modules
from agents.basic_Agent import BasicAgent
from dashboards.dashboard import Dashboard

app = dash.Dash('dMAS_controller', external_stylesheets=['https://codepen.io/chriddyp/pen/bWLwgP.css'])


class CtlAgent(BasicAgent):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        start_time = time.time()
        self.sim_start = False
        self.sim_stop = False

        self.start_date = pd.to_datetime('2018-01-01')
        self.stop_date = pd.to_datetime('2018-02-01')
        self.waiting_list = []
        self.cleared = False
        self.dashboard = Dashboard()
        self.simulation_interface.date = self.start_date

        self.logger.info(f'setup of the agent completed in {np.round(time.time() - start_time,2)} seconds')

    def set_waiting_list(self):
        self.waiting_list = self.simulation_interface.get_agents()
        self.logger.info(f'{len(self.waiting_list)} agent(s) are running')

    def wait_for_agents(self):
        t = time.time()
        total = len(self.waiting_list)
        still_waiting = len(self.waiting_list)
        with tqdm(total=total) as p_bar:
            display = True
            while len(self.waiting_list) > 0:
                if time.time() - t > 30 and display:
                    current_agents = self.simulation_interface.get_agents()
                    for agent in self.waiting_list:
                        if agent not in current_agents:
                            self.waiting_list.remove(agent)
                            self.logger.info(f'{agent} left the simulation stack')
                            self.logger.info(f'removed agent {agent} from current list')
                        else:
                            self.logger.info(f'still waiting for {agent}')
                        display = False
                if time.time() - t > 120:
                    for agent in self.waiting_list:
                        self.waiting_list.remove(agent)
                        self.logger.info(f'get no response of {agent}')
                        self.logger.info(f'removed agent {agent} from current list')
                p_bar.update(still_waiting - len(self.waiting_list))
                still_waiting = len(self.waiting_list)

                time.sleep(1)

    def wait_for_market(self):
        t = time.time()
        while not self.cleared:
            self.logger.info(f'still waiting for market clearing')
            time.sleep(1)
            if time.time() - t > 120:
                self.cleared = True
                pass
        self.cleared = False

    def callback(self, ch, method, properties, body):
        super().callback(ch, method, properties, body)
        message = body.decode("utf-8")
        agent, date = message.split(' ')
        date = pd.to_datetime(date)

        if date == self.date and agent in self.waiting_list:
            self.waiting_list.remove(agent)
        if agent == 'mrk_de111' and date == self.date:
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
                self.simulation_interface.initialize_tables()
                break
            else:
                try:
                    start_time = time.time()

                    self.date = date.date()

                    # 1.Step: optimization for dayAhead Market
                    self.set_waiting_list()
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

                    self.publish.basic_publish(exchange=self.mqtt_exchange, routing_key='',
                                               body=f'set_capacities {self.start_date.date()}')

                except Exception as e:
                    print(repr(e))
                    self.logger.exception('Error in Simulation')

        self.sim_start = False
        self.logger.info('simulation finished')

    def run(self):

        @app.callback(Output('information', 'children'), Input('tab_menu', 'value'))
        def render_information(tab):
            if tab == 'simulation':
                return self.dashboard.simulation_control(status=self.sim_start)
            if tab == 'meta':
                capacities = self.simulation_interface.get_global_capacities(self.date)
                return self.dashboard.meta_information(capacities)

        @app.callback(Output('simulation_status', 'children'),
                      Input('trigger_simulation', 'on'),
                      State('date_range', 'start_date'),
                      State('date_range', 'end_date'))
        def simulation_controlling(on, start, end):
            if on:
                self.start_date = pd.to_datetime(start)
                self.stop_date = pd.to_datetime(end)
                if not self.sim_start:
                    self.sim_stop = False
                    simulation = threading.Thread(target=self.simulation_routine)
                    check_orders = threading.Thread(target=self.check_orders)
                    simulation.start()
                    check_orders.start()
                    self.sim_start = True
            else:
                self.sim_stop = True
                self.logger.info('stopping simulation')
            return 'Simulation is running: {}.'.format(on)

        app.layout = self.dashboard.layout

        app.run_server(debug=False, port=5000, host='0.0.0.0')

