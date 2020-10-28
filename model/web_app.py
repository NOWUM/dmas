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

# model modules
from apps.misc_validData import write_valid_data, writeDayAheadError
from interfaces.interface_Influx import InfluxInterface as influxCon
from interfaces.interface_mongo import mongoInterface as mongoCon
from apps.view_grid import GridView

# 0. initialize variables
# ---------------------------------------------------------------------------------------------------------------------

path = os.path.dirname(os.path.dirname(__file__)) + r'/model'                  # change directory to cwd

gridView = GridView()                                                          # initialize grid view
config = configparser.ConfigParser()                                           # read config file
config.read('web_app.cfg')
market_port = config['Configuration']['marketport']
hostname = socket.gethostname()                                                # get computer name
ip_address = socket.gethostbyname(hostname)                                    # get ip address

app = Flask(__name__)                                                          # initialize App
CORS(app, resources={r"*": {"origins": "*"}})

# 1. methods for the web application
# ---------------------------------------------------------------------------------------------------------------------
@app.route('/')
def index():

    # Step 1: Load config values to find and initialize the infrastructure (databases and MQTT)
    system_conf = {key: value for key, value in config['Configuration'].items() if key != 'local'}#TODO: exclude 'marketport' to not show in Web App?  ( and key != 'marketport' )

    # Step 2:Load config values to build the agents
    agent_conf = {key.split('-')[0]: value for key, value in config.items() if 'Agent' in key}

    # Step 3: get agents, which are logged in
    mongo_connection = mongoCon(host=config['Configuration']['mongodb'],
                                database=config['Configuration']['database'])
    agents = mongo_connection.get_agents()
    mongo_connection.mongo.close()

    return render_template('index.html', **locals())


@app.route('/change_config', methods=['POST'])
def change_config():

    # Step 1: change configurations
    for key, value in request.form.to_dict().items():
        config['Configuration'][key] = value
    with open('web_app.cfg', 'w') as configfile:
        config.write(configfile)

    # Step 2: delete and clean up databases for new simulation
    if config.getboolean('Configuration', 'reset'):
        # clean influxdb
        influx_connection = influxCon(host=config['Configuration']['influxdb'],
                                      database=config['Configuration']['database'])
        try:
            influx_connection.influx.drop_database(config['Configuration']['database'])
        except Exception as e:
            print(e)
        influx_connection.influx.create_database(config['Configuration']['database'])
        influx_connection.influx.create_retention_policy(name=config['Configuration']['database'] + '_pol',
                                                         duration='INF', shard_duration='1d', replication=1)
        influx_connection.influx.close()
        # clean mongodb
        mongo_connection = mongoCon(host=config['Configuration']['mongodb'],
                                    database=config['Configuration']['database'])
        for name in mongo_connection.orderDB.list_collection_names():
            mongo_connection.orderDB.drop_collection(name)
        mongo_connection.mongo.close()

    return 'OK'


@app.route('/build', methods=['POST'])
def build_agents():

    # Step 1: get system configuration
    system_conf = {key: value for key, value in config['Configuration'].items() if key != 'local'}

    # Step 2: publish system configuration to server
    for typ in ['pwp', 'res', 'dem', 'str', 'net', 'mrk']:
        requests.post('http://' + str(request.form[typ + '_ip']) + ':5000/config', json=system_conf,  timeout=0.5)#TODO: set port (default:5000)?

    # Step 3: build agents on server
    for typ in ['pwp', 'res', 'dem', 'str', 'net', 'mrk']:
        data = {'typ': typ,
                'start': int(request.form[typ + '_start']),
                'end': int(request.form[typ + '_end'])}

        if typ == 'net' or typ == 'mrk':
            data.update({'start': 0})

        requests.post('http://' + str(request.form[typ + '_ip']) + ':5000/build', json=data, timeout=0.5)#TODO: set port (default:5000)?

    return 'OK'


@app.route('/Grid', methods=['GET', 'POST'])
def get_power_flow():
    try:
        if request.method == 'POST':
            fig = gridView.get_plot(date=pd.to_datetime(request.form['start']), hour=int(request.form['hour']))
            return render_template('tmp.html', plot=fig)
        else:
            fig = gridView.get_plot(date=pd.to_datetime('2018-01-01'), hour=0)
            return render_template('grid.html', plot=fig)
    except Exception as e:
        fig = None
        print('Exception in Grid:', e)
        return render_template('grid.html', plot=fig)


@app.route('/run', methods=['POST'])
def run():

    # Step 0: start simulation for start till end
    start = pd.to_datetime(request.form['start'])
    end = pd.to_datetime(request.form['end'])
    print('starts simulation from %s to %s: ' % (start.date(), end.date()))
    simulation(start, end)
    return 'OK'

@app.route('/kill_agents', methods=['POST'])
def kill_agents():
    # Kill Agents
    print('killing Agents...')
    rabbitmq_ip = config['Configuration']['rabbitmq']
    rabbitmq_exchange = config['Configuration']['exchange']
    # Step 2: check if rabbitmq runs local and choose the right login method
    if config.getboolean('Configuration', 'local'):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_ip, heartbeat=0))
    else:
        credentials = pika.PlainCredentials('dMAS', 'dMAS2020')
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_ip, heartbeat=0,
                                                                       credentials=credentials))
    send = connection.channel()  # declare Market Exchange
    send.exchange_declare(exchange=rabbitmq_exchange, exchange_type='fanout')
    send.basic_publish(exchange=rabbitmq_exchange, routing_key='', body='kill ')
    send.close()
    return 'OK'


def simulation(start, end, valid=True):

    # Step 1: connect to databases and mqtt
    database = config['Configuration']['database']
    mongo_connection = mongoCon(host=config['Configuration']['mongodb'], database=database)
    influx_connection = influxCon(host=config['Configuration']['influxdb'], database=database)

    rabbitmq_ip = config['Configuration']['rabbitmq']
    rabbitmq_exchange = config['Configuration']['exchange']

    # Step 2: check if rabbitmq runs local and choose the right login method
    if config.getboolean('Configuration', 'local'):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_ip, heartbeat=0))
    else:
        credentials = pika.PlainCredentials('dMAS', 'dMAS2020')
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_ip, heartbeat=0,
                                                                       credentials=credentials))
    send = connection.channel()  # declare Market Exchange
    send.exchange_declare(exchange=rabbitmq_exchange, exchange_type='fanout')

    # Step 3: delete and clean up databases for new simulation
    if config.getboolean('Configuration', 'reset'):
        # clean influxdb
        influx_connection = influxCon(host=config['Configuration']['influxdb'],
                                      database=config['Configuration']['database'])
        try:
            influx_connection.influx.drop_database(config['Configuration']['database'])
        except Exception as e:
            print(e)
        influx_connection.influx.create_database(config['Configuration']['database'])
        influx_connection.influx.create_retention_policy(name=config['Configuration']['database'] + '_pol',
                                                         duration='INF', shard_duration='1d', replication=1)
        # clean mongodb
        mongo_connection = mongoCon(host=config['Configuration']['mongodb'],
                                    database=config['Configuration']['database'])
        for name in mongo_connection.orderDB.list_collection_names():
            mongo_connection.orderDB.drop_collection(name)

    # Step 4: generate weather data for simulation time period
    influx_connection.generate_weather(start - pd.DateOffset(days=1), end + pd.DateOffset(days=1), valid)

    # Step 5: write valid data to measure the simulation quality
    if valid:
        print('write validation data')
        write_valid_data(database, 0, start, end)
        write_valid_data(database, 1, start, end)
        write_valid_data(database, 2, start, end)

    # Step 6: run simulation for each day
    for date in pd.date_range(start=start, end=end, freq='D'):

        try:
            start_time = tme.time()                                 # timestamp to measure simulation time
            # 1.Step: Run optimization for dayAhead Market
            send.basic_publish(exchange=rabbitmq_exchange, routing_key='', body='opt_dayAhead ' + str(date))

            # 2. Step: Run Market Clearing
            tme.sleep(5)                                            # wait 5 seconds before starting dayAhead clearing
            send.basic_publish(exchange=rabbitmq_exchange, routing_key='', body='dayAhead_clearing ' + str(date))
            while not mongoCon.get_market_status(date):                   # check if clearing done
                tme.sleep(1)

            # 3. Step: Run Power Flow calculation
            send.basic_publish(exchange=rabbitmq_exchange, routing_key='', body='grid_calc ' + str(date))

            # 4. Step: Publish Market Results
            send.basic_publish(exchange=rabbitmq_exchange, routing_key='', body='result_dayAhead ' + str(date))
            # if valid:
            #     writeDayAheadError(database, date)

            end_time = tme.time()-start_time
            print('Day %s complete in: %s seconds ' % (str(date.date()), end_time))

        except Exception as e:
            print('Error ' + str(date.date()))
            print(e)

    send.basic_publish(exchange=rabbitmq_exchange, routing_key='', body='kill ' + str('1970-01-01'))
    send.close()
    influx_connection.influx.close()
    mongo_connection.mongo.close()


if __name__ == "__main__":

    try:
        if config.getboolean('Configuration', 'local'):
            app.run(debug=False, port=market_port, host='127.0.0.1')
        else:
            app.run(debug=False, port=market_port, host=ip_address)
    except Exception as e:
        print(e)
    finally:
        exit()
