# third party modules
import time
import pandas as pd
from flask import Flask, request, redirect
import threading
import requests
import numpy as np

# model modules
from agents.basic_Agent import BasicAgent

app = Flask('dMAS_controller')


class CtlAgent(BasicAgent):

    def __init__(self, date, plz, agent_type, connect, infrastructure_source, infrastructure_login, *args, **kwargs):
        super().__init__(date, plz, agent_type, connect, infrastructure_source, infrastructure_login)
        self.logger.info('starting the agent')
        start_time = time.time()
        self.sim_start = False
        self.sim_stop = False

        self.start_date = pd.to_datetime('2018-01-01')
        self.stop_date = pd.to_datetime('2018-02-01')
        self.logger.info('setup of the agent completed in %s' % (time.time() - start_time))
        self.agent_list = []
        self.cleared = False

    def set_agents(self):
        headers = {'content-type': 'application/json', }
        response = requests.get('http://rabbitmq:15672/api/queues', headers=headers, auth=('guest', 'guest'))
        agents = response.json()
        for agent in agents:
            name = agent['name']
            if name[:3] in ['DEM', 'RES', 'STR', 'PWP']:
                self.agent_list.append(name)
        self.logger.info(f'{len(self.agent_list)} agent(s) are running')

    def get_agents(self):
        headers = {'content-type': 'application/json', }
        response = requests.get('http://rabbitmq:15672/api/queues', headers=headers, auth=('guest', 'guest'))
        agents = response.json()
        return [agent['name'] for agent in agents if agent['name'][:3] in ['DEM', 'RES', 'STR', 'PWP']]


    def wait_for_agents(self):
        t = time.time()
        while len(self.agent_list) > 0:
            time.sleep(5)
            if len(self.agent_list) > 0:
                self.logger.info(f'still waiting for: {self.agent_list}')
            if time.time() - t > 30:
                current_agents = self.get_agents()
                for agent in self.agent_list:
                    if agent not in current_agents:
                        self.agent_list.remove(agent)
                        self.logger.info(f'get no response of {agent}')
                        self.logger.info(f'removed agent {agent} from waiting list')


    def wait_for_market(self):
        while not self.cleared:
            self.logger.info(f'still waiting for market clearing')
            time.sleep(1)
        self.cleared = False

    def callback(self, ch, method, properties, body):
        super().callback(ch, method, properties, body)
        message = body.decode("utf-8")
        agent, date = message.split(' ')
        date = pd.to_datetime(date)
        if date == self.date and agent in self.agent_list:
            self.logger.info(f'Agent {agent} set orders')
            self.agent_list.remove(agent)
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

        for date in pd.date_range(start=self.start_date, end=self.stop_date, freq='D'):
            if self.sim_stop:
                self.logger.info('simulation terminated')
                break
            else:
                try:
                    start_time = time.time()

                    self.date = date.date()

                    # 1.Step: optimization for dayAhead Market
                    self.set_agents()
                    self.publish.basic_publish(exchange=self.exchange_name, routing_key='',
                                               body=f'opt_dayAhead {date.date()}')

                    self.wait_for_agents()
                    self.logger.info(f'{len(self.get_agents())} agents set their order books')

                    # 2. Step: run market clearing
                    self.publish.basic_publish(exchange=self.exchange_name, routing_key='',
                                               body=f'dayAhead_clearing {date.date()}')

                    self.wait_for_market()
                    self.logger.info(f'day ahead clearing finished')

                    # 3. Step: agents have to adjust their demand and generation
                    self.publish.basic_publish(exchange=self.exchange_name, routing_key='',
                                               body=f'result_dayAhead {date.date()}')

                    # 4. Step: reset the order_book table for the next day
                    self.simulation_database.execute("DELETE FROM order_book")

                    self.logger.info(f'finished day in {np.round(time.time() - start_time, 2)} seconds')

                    #self.publish.basic_publish(exchange=self.exchange_name, routing_key='',
                    #                           body=f'set_capacities {self.start_date.date()}')

                    # 3. Step: Run Power Flow calculation
                    #self.publish.basic_publish(exchange=self.exchange_name, routing_key='',
                    #                           body=f'grid_calc {date.date()}')

                except Exception as e:
                    print(repr(e))
                    self.logger.exception('Error in Simulation')

        self.sim_start = False
        self.logger.info('simulation finished')

    def run(self):

        query = '''CREATE TABLE order_book (block_id bigint, hour bigint, order_id bigint, name text, price double precision, 
                                            volume double precision, link bigint, type text)'''
        self.simulation_database.execute(query)
        self.simulation_database.execute('ALTER TABLE "order_book" ADD PRIMARY KEY ("block_id", "hour", "order_id", "name");')
        query = '''CREATE TABLE capacities ("time" timestamp without time zone, bio double precision, 
                                            coal double precision, gas double precision, lignite double precision,
                                            nuclear double precision, solar double precision, water double precision,
                                            wind double precision, storage double precision, agent text)'''
        self.simulation_database.execute(query)
        self.simulation_database.execute('ALTER TABLE "capacities" ADD PRIMARY KEY ("time", "agent");')
        query = '''CREATE TABLE demand ("time" timestamp without time zone, power double precision,
                                        heat double precision, step text, agent text)'''
        self.simulation_database.execute(query)
        self.simulation_database.execute('ALTER TABLE "demand" ADD PRIMARY KEY ("time", "step", "agent");')
        query = '''CREATE TABLE generation ("time" timestamp without time zone, total double precision,
                                            solar double precision, wind double precision, water double precision,
                                            bio double precision, lignite double precision, coal double precision,
                                            gas double precision, nuclear double precision, step text,
                                            agent text)'''
        self.simulation_database.execute(query)
        self.simulation_database.execute(f'ALTER TABLE "generation" ADD PRIMARY KEY ("time", "step", "agent");')

        query = '''CREATE TABLE auction_result ("time" timestamp without time zone, price double precision,
                                                volume double precision)'''
        self.simulation_database.execute(query)
        self.simulation_database.execute(f'ALTER TABLE "auction_result" ADD PRIMARY KEY ("time");')

        query = '''CREATE TABLE market_results (block_id bigint, hour bigint, order_id bigint, name text, price double precision, 
                                                volume double precision, link bigint, type text)'''
        self.simulation_database.execute(query)
        self.simulation_database.execute('ALTER TABLE "market_results" ADD PRIMARY KEY ("block_id", "hour", "order_id", "name");')

        @app.route('/')
        def main_page():
            if not self.sim_start:
                content = '''
                <form method="POST" action="/start" style="display: flex;flex-direction: column;">
                    <span style="margin-bottom: 20px;">
                        <label for="start_date">Start Date</label><br>
                        <input type="date" id="start_date" name="start_date" value="1995-01-01" />
                    </span>
                    <span style="margin-bottom: 20px;">
                        <label for="end_date">End Date</label><br>
                        <input type="date" id="end_date" name="end_date" value="1995-02-01" />
                    </span>
                    <input type="submit" value="Start Simulation">
                </form>'''
            else:
                content = '''
                <form method="POST" action="/stop" style="display: flex;flex-direction: column;">
                    <input type="submit" value="Stop Simulation">
                </form>'''
            return f'''
                <div center style="width: 60%; margin: auto; height: 80%" >
                <h1>Docker Agent-based Simulation</h1>
                Simulation running: {self.sim_start}
                {content}

                </div>
                '''

        @app.route('/stop')
        def stop_simulation():
            self.sim_stop = True
            self.logger.info('stopping simulation')
            return redirect('/')

        @app.route('/start', methods=['POST'])
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

        app.run(debug=False, host="0.0.0.0", port=5000)
