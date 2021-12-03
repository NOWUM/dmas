# third party modules
import time
import pandas as pd
from flask import Flask, request
import threading
import requests

# model modules
from agents.basic_Agent import BasicAgent

app = Flask('dMAS_controller')


class CtlAgent(BasicAgent):

    def __init__(self, date, plz, agent_type, mqtt_exchange, connect,  infrastructure_source, infrastructure_login):
        super().__init__(date, plz, agent_type, mqtt_exchange, connect, infrastructure_source, infrastructure_login)
        self.logger.info('starting the agent')
        start_time = time.time()
        self.sim_start = False
        self.sim_stop = False

        self.start_date = '2018-01-01'
        self.stop_date = '2018-02-01'
        self.logger.info('setup of the agent completed in %s' % (time.time() - start_time))
        self.agent_list = {}

    def callback(self, ch, method, properties, body):
        super().callback(ch, method, properties, body)
        message = body.decode("utf-8")
        agent, status = message.split(' ')
        self.agent_list[agent] = status

    def check_orders(self):
        self.channel.basic_consume(queue=self.name, on_message_callback=self.callback, auto_ack=True)
        print(' --> Waiting for orders'
              % self.name)
        self.channel.start_consuming()

    def simulation_routine(self):
        self.logger.info('simulation started')
        for date in pd.date_range(start=self.start_date, end=self.stop_date, freq='D'):
            if self.sim_stop:
                self.logger.info('simulation terminated')
                break
            else:
                try:
                    start_time = time.time()  # timestamp to measure simulation time
                    # 1.Step: Run optimization for dayAhead Market
                    self.channel.basic_publish(exchange=self.exchange, routing_key='', body=f'opt_dayAhead {date}')
                    orders_setting = True
                    while orders_setting:
                        time.sleep(0.1)
                        if all([values for _, values in self.agent_list.items()]):
                            orders_setting = False
                    # 2. Step: Run Market Clearing
                    self.channel.basic_publish(exchange=self.exchange, routing_key='', body=f'dayAhead_clearing {date}')
                    # 3. Step: Run Power Flow calculation
                    self.channel.basic_publish(exchange=self.exchange, routing_key='', body=f'grid_calc {date}')
                    # 4. Step: Publish Market Results
                    self.channel.basic_publish(exchange=self.exchange, routing_key='', body=f'result_dayAhead {date}')
                    # 5. Step: Rest agent list for next day
                    for key, _ in self.agent_list.items():
                        self.agent_list[key] = False
                    # TODO: for first day add primary keys to tabels
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
            if self.sim_start:
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
            return 'OK'

        @app.route('/run', methods=['POST'])
        def run_simulation():
            rf = request.form
            self.start_date = rf.get('start', '2018-01-01')
            self.stop_date = rf.get('stop', '2018-02-01')

            if not self.sim_start:
                headers = {'content-type': 'application/json',}
                response = requests.get('rabbitmq:15672/api/queues', headers=headers, auth=('guest', 'guest'))
                agents = response.json()
                print(agents)
                simulation = threading.Thread(target=self.simulation_routine)
                check_orders = threading.Thread(target=self.check_orders)
                simulation.start()
                check_orders.start()
                self.sim_start = True

            return 'OK'

        app.run(debug=False, host="0.0.0.0", port=5000)


