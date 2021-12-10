# third party modules
import time
import pandas as pd
from flask import Flask, request, redirect
import threading
import requests

# model modules
from agents.basic_Agent import BasicAgent

app = Flask('dMAS_controller')


class CtlAgent(BasicAgent):

    def __init__(self, date, plz, agent_type, connect,  infrastructure_source, infrastructure_login, *args, **kwargs):
        super().__init__(date, plz, agent_type, connect, infrastructure_source, infrastructure_login)
        self.logger.info('starting the agent')
        start_time = time.time()
        self.sim_start = False
        self.sim_stop = False

        self.start_date = '2018-01-01'
        self.stop_date = '2018-02-01'
        self.logger.info('setup of the agent completed in %s' % (time.time() - start_time))
        self.agent_list = []
        self.cleared = False

    def get_agents(self):
        headers = {'content-type': 'application/json', }
        response = requests.get('http://rabbitmq:15672/api/queues', headers=headers, auth=('guest', 'guest'))
        agents = response.json()
        for agent in agents:
            name = agent['name']
            if name[:3] in ['DEM', 'RES', 'STR', 'PWP']:
                self.agent_list.append(name)

    def callback(self, ch, method, properties, body):
        super().callback(ch, method, properties, body)
        message = body.decode("utf-8")
        agent, date = message.split(' ')
        date = pd.to_datetime(date)
        if date == self.date:
            self.agent_list.remove(agent)
        if agent == 'MRK_1' and date == self.date:
            self.cleared = True

    def check_orders(self):
        try:
            self.channel.basic_consume(queue=self.name, on_message_callback=self.callback, auto_ack=True)
            print(' --> Waiting for orders'
                  % self.name)
            self.channel.start_consuming()
        except Exception as e:
            print(repr(e))

    def simulation_routine(self):
        self.logger.info('simulation started')
        self.agent_list = self.get_agents()
        for date in pd.date_range(start=self.start_date, end=self.stop_date, freq='D'):
            if self.sim_stop:
                self.logger.info('simulation terminated')
                break
            else:
                try:
                    start_time = time.time()  # timestamp to measure simulation time
                    # 1.Step: Run optimization for dayAhead Market
                    self.publish.basic_publish(exchange=self.exchange_name, routing_key='', body=f'opt_dayAhead {date}')
                    while len(self.agent_list) > 0:
                        time.sleep(1)
                    # 2. Step: Run Market Clearing
                    self.publish.basic_publish(exchange=self.exchange_name, routing_key='', body=f'dayAhead_clearing {date}')
                    # 3. Step: Run Power Flow calculation
                    self.publish.basic_publish(exchange=self.exchange_name, routing_key='', body=f'grid_calc {date}')
                    while not self.cleared:
                        time.sleep(1)
                    # 4. Step: Publish Market Results
                    self.publish.basic_publish(exchange=self.exchange_name, routing_key='', body=f'result_dayAhead {date}')
                    # 5. Step: Rest agent list and cleared flag for next day
                    self.agent_list = self.get_agents()
                    self.cleared = False

                    # TODO: for first day add primary keys to tables
                    end_time = time.time() - start_time
                    self.logger.info('Day %s complete in: %s seconds ' % (str(date.date()), end_time))
                except Exception as e:
                    print(repr(e))
                    self.logger.exception('Error in Simulation')

        self.sim_start = False
        self.logger.info('simulation finished')

    def run(self):
        @app.route('/')
        def main_page():
            if not self.sim_start:
                content = '''
                <form method="POST" action="/start" style="display: flex;flex-direction: column;">
                    <span style="margin-bottom: 20px;">
                        <label for="start_date">Start Date</label><br>
                        <data type="date" id="start_date" name="start_date" value="1995-01-01" />
                    </span>
                    <span style="margin-bottom: 20px;">
                        <label for="end_date">End Date</label><br>
                        <data type="date" id="end_date" name="end_date" value="1995-02-01" />
                    </span>
                    <data type="submit" value="Start Simulation">
                </form>'''
            else:
                content = '''
                <form method="POST" action="/stop" style="display: flex;flex-direction: column;">
                    <data type="submit" value="Stop Simulation">
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
            self.start_date = rf.get('start', '2018-01-01')
            self.stop_date = rf.get('stop', '2018-02-01')

            if not self.sim_start:
                simulation = threading.Thread(target=self.simulation_routine)
                check_orders = threading.Thread(target=self.check_orders)
                simulation.start()
                check_orders.start()
                self.sim_start = True

            return redirect('/')

        app.run(debug=False, host="0.0.0.0", port=5000)


