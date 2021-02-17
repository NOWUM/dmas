# third party modules
import configparser
import os
from sys import exit
import time as tme
import pandas as pd
import pika
from flask import Flask, render_template, request
from flask_cors import CORS
import requests
import socket
import json
import plotly.graph_objects as go
import plotly.express as px
import plotly.io as pio
import numpy as np

# model modules
from apps.misc_GenWeather import WeatherGenerator
from apps.misc_validData import write_valid_data, writeDayAheadError
from interfaces.interface_Influx import InfluxInterface as influxCon
from interfaces.interface_mongo import mongoInterface as mongoCon
from apps.view_grid import GridView

# 0. initialize variables
# ---------------------------------------------------------------------------------------------------------------------
path = os.path.dirname(os.path.dirname(__file__)) + r'/model'                  # change directory to cwd

gridView = GridView()                                                          # initialize grid view
config = configparser.ConfigParser()                                           # read config file
config.read('control_service.cfg')

hostname = socket.gethostname()                                                # get computer name
ip_address = socket.gethostbyname(hostname)                                    # get ip address

app = Flask(__name__)
CORS(app, resources={r"*": {"origins": "*"}})

@app.route('/')
def index():
    return 'Service running'

@app.route('/test_plotly')
def test_plotly():
    # fig = px.scatter(x=[0, 1, 2, 3, 4], y=[0, 1, 30, 9, 40])
    fig = go.Figure()

    # Add traces, one for each slider step
    for step in np.arange(0, 5, 0.1):
        fig.add_trace(
            go.Scatter(
                visible=False,
                line=dict(color="#00CED1", width=6),
                name="𝜈 = " + str(step),
                x=np.arange(0, 10, 0.01),
                y=np.sin(step * np.arange(0, 10, 0.01))))

    # Make 10th trace visible
    fig.data[10].visible = True

    # Create and add slider
    steps = []
    for i in range(len(fig.data)):
        step = dict(
            method="update",
            args=[{"visible": [False] * len(fig.data)},
                  {"title": "Slider switched to step: " + str(i)}],  # layout attribute
        )
        step["args"][0]["visible"][i] = True  # Toggle i'th trace to "visible"
        steps.append(step)

    sliders = [dict(
        active=10,
        currentvalue={"prefix": "Frequency: "},
        pad={"t": 50},
        steps=steps
    )]

    fig.update_layout(
        sliders=sliders
    )

    return fig.to_json()# json.dumps('OK')

@app.route('/GridGra', methods=['GET','POST'])
def grid_grafana():
    # try:
    #     if request.method == 'POST':
    #         date = request.form['start']
    #         hour = int(request.form['hour'])
    #         fig = gridView.get_plot(date=pd.to_datetime(date), hour=hour)
    #         #return render_template('tmp.html', plot=fig)
    #         return fig
    #     else:
    #         fig = gridView.get_plot(date=pd.to_datetime('2018-01-01'), hour=0)
    #         return fig
    # except Exception as e:
    #     fig = None
    #     print('Exception in Grid:', e)
    #     #return render_template('grid.html', plot=fig)
    #     return fig
    # fig = px.scatter(x=[0, 1, 2, 3, 4], y=[0, 1, 30, 9, 40])
    fig = go.Figure()

    # Add traces, one for each slider step
    for step in np.arange(0, 5, 0.1):
        fig.add_trace(
            go.Scatter(
                visible=False,
                line=dict(color="#00CED1", width=6),
                name="𝜈 = " + str(step),
                x=np.arange(0, 10, 0.01),
                y=np.sin(step * np.arange(0, 10, 0.01))))

    # Make 10th trace visible
    fig.data[10].visible = True

    # Create and add slider
    steps = []
    for i in range(len(fig.data)):
        step = dict(
            method="update",
            args=[{"visible": [False] * len(fig.data)},
                  {"title": "Slider switched to step: " + str(i)}],  # layout attribute
        )
        step["args"][0]["visible"][i] = True  # Toggle i'th trace to "visible"
        steps.append(step)

    sliders = [dict(
        active=10,
        currentvalue={"prefix": "Frequency: "},
        pad={"t": 50},
        steps=steps
    )]

    fig.update_layout(
        sliders=sliders
    )

    return fig.to_json()# json.dumps('OK')

@app.route('/Grid', methods=['GET','POST'])
#@cross_origin()
def grid():
    try:
        if request.method == 'POST':
            date = request.form['start']
            hour = int(request.form['hour'])
            fig = gridView.get_plot(date=pd.to_datetime(date), hour=hour)
            return render_template('tmp.html', plot=fig)
        else:
            fig = gridView.get_plot(date=pd.to_datetime('2018-01-01'), hour=0)
            return render_template('grid.html', plot=fig)
    except Exception as e:
        fig = None
        print('Exception in Grid:', e)
        return render_template('grid.html', plot=fig)

@app.route('/get_config/<typ>', methods=['GET'])
def get_config(typ='services'):
    if typ == 'services':
        return_config = {key: value for key, value in config['Configuration'].items()}
    else:
        agent_conf = ({key.split('-')[0]: dict(value) for key, value in config.items() if 'Agent' in key})
        return_config = agent_conf[typ]

    return json.dumps(return_config)

@app.route('/set_config/<typ>', methods=['POST'])
def set_config(typ='services'):
    # change configurations and save to file
    if typ == 'services':
        config['Configuration'].update(json.loads(request.data))
    else:
        config['%s-Agent' %typ].update(json.loads(request.data))

    with open('control_service.cfg', 'w') as configfile:
        config.write(configfile)

    return json.dumps('OK')

@app.route('/get_info/<typ>', methods=['GET'])
def get_info(typ='services'):
    print('Get Info of Agent Typ:', typ)
    # -- init database
    database = config['Configuration']['database']
    mongo_connection = mongoCon(host=config['Configuration']['mongodb'], database=database)
    agents = mongo_connection.get_agents_ip(typ)
    mongo_connection.mongo.close()
    return json.dumps(agents)

@app.route('/get_running_agents/<typ>', methods=['GET'])
def get_running_agents(typ='services'):
    # -- init database
    database = config['Configuration']['database']
    mongo_connection = mongoCon(host=config['Configuration']['mongodb'], database=database)
    agents = mongo_connection.get_agents_ip(typ)
    mongo_connection.mongo.close()

    return json.dumps(len(agents))

@app.route('/terminate_agent/<key>', methods=['GET'])
def terminate_agent(key=None):
    print('Killing Agent with key:', key)
    # -- int MQTT
    rabbitmq_ip = config['Configuration']['rabbitmq']
    rabbitmq_exchange = config['Configuration']['exchange']
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=rabbitmq_ip, virtual_host='SimAgent', heartbeat=0,
                                  credentials=pika.PlainCredentials('dMAS', 'dMAS2020')))
    send = connection.channel()
    send.exchange_declare(exchange=rabbitmq_exchange, exchange_type='fanout')
    send.basic_publish(exchange=rabbitmq_exchange, routing_key='', body=str(key) + ' 1970-01-01')
    send.close()

    return json.dumps('OK')

@app.route('/terminate_agents/<typ>', methods=['GET'])
def terminate_agents(typ=None):
    print('Killing Agents of typ:', typ)
    # -- int MQTT
    rabbitmq_ip = config['Configuration']['rabbitmq']
    rabbitmq_exchange = config['Configuration']['exchange']
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_ip, virtual_host='SimAgent',heartbeat=0,
                                                                   credentials=pika.PlainCredentials('dMAS', 'dMAS2020')))
    send = connection.channel()
    send.exchange_declare(exchange=rabbitmq_exchange, exchange_type='fanout')
    send.basic_publish(exchange=rabbitmq_exchange, routing_key='', body=str(typ) + '_all ' + '1970-01-01')
    send.close()

    return json.dumps('OK')

@app.route('/start_agents/<typ>', methods=['GET'])
def start_agents(typ=None):
    agent_conf = ({key.split('-')[0]: dict(value) for key, value in config.items() if 'Agent' in key})
    host = agent_conf[typ]['host']              # Server where agent should run
    start = int(agent_conf[typ]['start'])       # starting area
    if 'stop' in agent_conf[typ].keys():
        stop = int(agent_conf[typ]['stop'])     # ending area
        print('Start Agents of typ:', typ, 'on host', host, 'from', start, 'to', stop)
    else:
        stop = start
        start = 0
        print('Start Agents of typ:', typ, 'on host', host)
    data = {'typ': typ, 'start': start, 'end': stop}

    # Step 1: set system configuration on server
    system_conf = {key: value for key, value in config['Configuration'].items()}
    #print(system_conf)
    requests.post('http://' + str(host) + ':5000/config', json=system_conf, timeout=0.5)
    # Step 2: start agents
    requests.post('http://' + str(host) + ':5000/build', json=data, timeout=0.5)
    #print(data)

    return json.dumps('OK')

@app.route('/start_simulation', methods=['POST'])
def start_simulation():
    valid = True

    sim_time = json.loads(request.data)
    start = pd.to_datetime(sim_time['start'])
    end = pd.to_datetime(sim_time['end'])

    print('starting Simulation from', start.date(), 'to', end.date())

    # Step 1: prepare databases and MQTT
    database = config['Configuration']['database']  # database name
    weather_generator = WeatherGenerator(database=database, host=config['Configuration']['influxdb'])

    # -- init influx database
    i_con = influxCon(host=config['Configuration']['influxdb'], database=database)
    if database in [x['name'] for x in i_con.influx.get_list_database()]:
        i_con.influx.drop_database(database)
    i_con.influx.create_database(database)
    i_con.influx.create_retention_policy(name=database + '_pol',
                                         duration='INF', shard_duration='3000d', replication=1)
    # -- init mongo database
    m_con = mongoCon(host=config['Configuration']['mongodb'], database=database)
    [m_con.orderDB.drop_collection(name) for name in m_con.orderDB.list_collection_names() if name != 'status']
    # -- int MQTT
    rabbitmq_ip = config['Configuration']['rabbitmq']
    rabbitmq_exchange = config['Configuration']['exchange']
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_ip, virtual_host='SimAgent',heartbeat=0,
                                                                   credentials=pika.PlainCredentials('dMAS', 'dMAS2020')))
    send = connection.channel()
    send.exchange_declare(exchange=rabbitmq_exchange, exchange_type='fanout')

    # Step 2: generate weather data for simulation time period
    # i_con.generate_weather(start - pd.DateOffset(days=1), end + pd.DateOffset(days=1), valid)

    # Step 3: write validation data
    if valid:
        print('write validation data')
        write_valid_data(database, 0, start, end)
        write_valid_data(database, 1, start, end)
        write_valid_data(database, 2, start, end)

    # Step 4: run simulation for each day
    weather_generator.generate_weather(valid=valid, date=pd.to_datetime(start))
    gen_weather = True
    for date in pd.date_range(start=start, end=end, freq='D'):

        try:
            start_time = tme.time()  # timestamp to measure simulation time
            # 1.Step: Run optimization for dayAhead Market
            send.basic_publish(exchange=rabbitmq_exchange, routing_key='', body='opt_dayAhead ' + str(date))
            # 2. Step: Run Market Clearing
            send.basic_publish(exchange=rabbitmq_exchange, routing_key='', body='dayAhead_clearing ' + str(date))
            while not m_con.get_market_status(date):  # check if clearing done
                if gen_weather:
                    weather_generator.generate_weather(valid, date + pd.DateOffset(days=1))
                    gen_weather = False
                else:
                    tme.sleep(1)
            # 3. Step: Run Power Flow calculation
            send.basic_publish(exchange=rabbitmq_exchange, routing_key='', body='grid_calc ' + str(date))
            # 4. Step: Publish Market Results
            send.basic_publish(exchange=rabbitmq_exchange, routing_key='', body='result_dayAhead ' + str(date))

            gen_weather = True

            end_time = tme.time() - start_time
            print('Day %s complete in: %s seconds ' % (str(date.date()), end_time))

        except Exception as e:
            print('Error ' + str(date.date()))
            print(e)

    send.close()
    i_con.influx.close()
    m_con.mongo.close()

    return json.dumps('OK')

if __name__ == "__main__":

    print('starting service')
    app.run(debug=False, port=5010, host=ip_address)
