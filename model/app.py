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

path = os.path.dirname(os.path.dirname(__file__)) + r'/model'                  # change directory

pd.set_option('mode.chained_assignment', None)                                 # ignore pd warnings

gridView = GridView()                                                          # initialize grid view
config = configparser.ConfigParser()                                           # read config file
config.read('app.cfg')

hostname = socket.gethostname()
ip_address = socket.gethostbyname(hostname)

app = Flask(__name__)                                                          # initialize App
cors = CORS(app, resources={r"*": {"origins": "*"}})


# 1. methods for the web application
# ---------------------------------------------------------------------------------------------------------------------
@app.route('/')
def index():

    # load config values Infrastructure
    # -------------------------------------------------------
    system_conf = {}
    for key, value in config['Configuration'].items():
        if key != 'local':
            system_conf.update({key: value})
        # if (key == 'mongodb' or key == 'influxdb' or key == 'rabbitmq') and value not in server_list:
        #    server_list.append(value)

    # load config values Agents
    # -------------------------------------------------------
    server_list = {}
    agent_conf = {}
    for key, value in config.items():
        if 'Agent' in key:
            dict_ = {}
            for k, v in value.items():
                dict_.update({k: v})
                if k == 'host' and v not in server_list:
                    server_list.update({v: 'not available'})
            x = key.split('-')[0]
            agent_conf.update({x: dict_})


    # check if simulation servers are running:
    # -------------------------------------------------------
    for server, _ in server_list.items():
        try:
            res = requests.get('http://' + server + ':5000/test')
            if res.status_code == 200:
                server_list.update({server: 'available'})
        except Exception as e:
            print(e)
    # get agents, which are logged in
    # -------------------------------------------------------
    mongo_connection = mongoCon(host=config['Configuration']['MongoDB'],
                                database=config['Configuration']['Database'])
    agent_ids = mongo_connection.status.find().distinct('_id')
    mongo_connection.mongo.close()

    agents = {}
    for typ in ['PWP', 'RES', 'DEM', 'STR', 'NET', 'MRK']:
        counter = 0
        for id in agent_ids:
            if typ == id.split('_')[0]:
                counter += 1
        agents.update({typ: counter})

    return render_template('index.html', **locals())


@app.route('/change_config', methods=['POST'])
def change_config():

    # change configurations
    # ----------------------------------------------------------------
    for key, value in request.form.to_dict().items():
        config['Configuration'][key] = value
    with open('app.cfg', 'w') as configfile:
        config.write(configfile)

    # delete and clean up databases for new simulation
    # ----------------------------------------------------------------
    if config.getboolean('Configuration', 'Reset'):
        # clean influxdb
        influx_connection = influxCon(host=config['Configuration']['influxDB'],
                                      database=config['Configuration']['Database'])
        try:
            influx_connection.influx.drop_database(config['Configuration']['Database'])
        except Exception as e:
            print(e)
        influx_connection.influx.create_database(config['Configuration']['Database'])
        influx_connection.influx.create_retention_policy(name=config['Configuration']['Database'] + '_pol',
                                                         duration='INF', shard_duration='1d', replication=1)
        influx_connection.influx.close()
        # clean mongodb
        mongo_connection = mongoCon(host=config['Configuration']['mongoDB'],
                                    database=config['Configuration']['Database'])
        for name in mongo_connection.orderDB.list_collection_names():
            mongo_connection.orderDB.drop_collection(name)
        mongo_connection.mongo.close()

    return 'OK'


@app.route('/build', methods=['POST'])
def build_agents():

    system_conf = {}
    for key, value in config['Configuration'].items():
        if key != 'suffix' and key != 'local':
            system_conf.update({key: value})

    # set agent configs
    for typ in ['pwp', 'res', 'dem', 'str', 'net', 'mrk']:
        key = typ + '_ip'
        url = 'http://' + str(request.form[key]) + ':5000/config'
        requests.post(url, json=system_conf)

    # build agents
    for typ in ['pwp', 'res', 'dem', 'str', 'net', 'mrk']:
        key = typ + '_ip'
        url = 'http://' + str(request.form[key]) + ':5010/build'

        key_s = typ + '_start'
        key_e = typ + '_end'

        if typ != 'net' and typ != 'mrk':
            data = {'typ':         typ,
                    'start':       int(request.form[key_s]),
                    'end':         int(request.form[key_e])}
        else:

            data = {'typ':         typ,
                    'start':       0,
                    'end':         int(request.form[key_e] == 'true')}

        requests.post(url, json=data)

    return 'OK'


@app.route('/Grid', methods=['GET', 'POST'])
def get_power_flow():
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


@app.route('/run', methods=['POST'])
def run():
    # start simulation for start till end
    # --------------------------------------------------------------
    start = pd.to_datetime(request.form['start'])
    end = pd.to_datetime(request.form['end'])
    print('starts simulation from %s to %s: ' % (start.date(), end.date()))
    simulation(start, end)
    return 'OK'


def simulation(start, end, valid=True):

    database = config['Results']['Database']

    # connection to databases
    # --------------------------------------------------------------
    mongo_connection = mongoCon(host=config['MongoDB']['Host'],
                                database=config['Results']['Database'])
    influx_connection = influxCon(host=config['InfluxDB']['Host'],
                                  database=config['Results']['Database'])

    # connection to MQTT
    # --------------------------------------------------------------
    rabbitmq_ip = config['RabbitMQ']['Host']                            # host IP
    rabbitmq_exchange = config['RabbitMQ']['Exchange']                  # exchange name

    # check if rabbitmq runs local and choose the right login method
    # --------------------------------------------------------------
    if config.getboolean('RabbitMQ', 'Local'):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_ip, heartbeat=0))
    else:
        credentials = pika.PlainCredentials('dMAS', 'dMAS2020')
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_ip, heartbeat=0,
                                                                       credentials=credentials))
    send = connection.channel()  # declare Market Exchange
    send.exchange_declare(exchange=rabbitmq_exchange, exchange_type='fanout')

    # delete and clean up databases for new simulation
    # ----------------------------------------------------------------
    if config.getboolean('Results', 'Delete'):
        # clean influxdb
        try:
            influx_connection.influx.drop_database(request.form['result_db'])
        except Exception as e:
            print(e)
        influx_connection.influx.create_database(request.form['result_db'])
        influx_connection.influx.create_retention_policy(name=request.form['result_db'] + '_pol', duration='INF',
                                                         shard_duration='1d', replication=1)
        # clean mongodb
        for name in mongo_connection.orderDB.list_collection_names():
            mongo_connection.orderDB.drop_collection(name)

    # generate weather data for simulation time period
    # ----------------------------------------------------------------
    influx_connection.generate_weather(start - pd.DateOffset(days=1), end + pd.DateOffset(days=1), valid)

    # write valid data to measure the simulation quality
    # ----------------------------------------------------------------
    if valid:
        print('write validation data')
        write_valid_data(database, 0, start, end)
        write_valid_data(database, 1, start, end)
        write_valid_data(database, 2, start, end)

    # run simulation for each day
    # ----------------------------------------------------------------
    for date in pd.date_range(start=start, end=end, freq='D'):

        mongo_connection.orderDB[str(date.date)]

        try:
            start_time = tme.time()                                 # timestamp to measure simulation time
            # 1.Step: Run optimization for dayAhead Market
            # -------------------------------------------------------
            send.basic_publish(exchange=rabbitmq_exchange, routing_key='', body='opt_dayAhead ' + str(date))

            # 2. Step: Run Market Clearing
            # -------------------------------------------------------
            tme.sleep(5)                                            # wait 5 seconds before starting dayAhead clearing
            send.basic_publish(exchange=rabbitmq_exchange, routing_key='', body='dayAhead_clearing ' + str(date))
            while not mongoCon.getClearing(date):                   # check if clearing done
                tme.sleep(1)

            # 3. Step: Run Power Flow calculation
            # -------------------------------------------------------
            send.basic_publish(exchange=rabbitmq_exchange, routing_key='', body='grid_calc ' + str(date))

            # 4. Step: Publish Market Results
            # -------------------------------------------------------
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
        if config.getboolean('Configuration', 'Local'):
            app.run(debug=False, port=5010, host='127.0.0.1')
        else:
            app.run(debug=False, port=5010, host=ip_address)
    except Exception as e:
        print(e)
    finally:
        exit()
