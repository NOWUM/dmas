# third party modules
import configparser
import os
import subprocess
import time as tme
import pandas as pd
import pika
import psutil
from flask import Flask, render_template, request
from flask_cors import cross_origin

# model modules
from apps.routine_DayAhead import da_clearing
from apps.misc_validData import write_valid_data, writeDayAheadError
from interfaces.interface_Influx import InfluxInterface as influxCon
from interfaces.interface_mongo import mongoInterface as mongoCon
from apps.view_grid import GridView


"""
    declare global variables for the application
"""

path = os.path.dirname(os.path.dirname(__file__)) + r'/model'                                                       # change working directory

pd.set_option('mode.chained_assignment', None)                                                                      # ignore pd warnings

config = configparser.ConfigParser()                                                                                # read config file
config.read('app.cfg')

database = config['Results']['Database']                                                                            # name of influxdatabase to store the results
mongoCon = mongoCon(host=config['MongoDB']['Host'], database=database)                                              # connection and interface to MongoDB
influxCon = influxCon(host=config['InfluxDB']['Host'], database=database)                                           # connection and interface to InfluxDB
marketIp = config['Market']['Host']
marketPort = config['Market']['Port']
exchange = config['Market']['Exchange']


if config.getboolean('Market', 'Local'):                                                                            # check if plattform runs local and choose the right login method
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=config['Market']['Host'], heartbeat=0))
else:
    credentials = pika.PlainCredentials('dMAS', 'dMAS2020')
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=config['Market']['Host'],
                                                                   heartbeat=0, credentials=credentials))

send = connection.channel()                                                                                         # declare Market Excahnge
send.exchange_declare(exchange=exchange, exchange_type='fanout')

app = Flask(__name__)

gridView = GridView()

"""
    methods for the web application
"""

@app.route('/')
def index():
    """
    default view with agent counter
    :return: index.html in templates
    """
    num = []
    agent_ids = mongoCon.status.find().distinct('_id')

    for typ in ['PWP', 'RES', 'DEM', 'STR']:
        counter = 0
        for id_ in agent_ids:
            if typ == id_.split('_')[0]:
                counter += 1
        num.append(counter)

    return render_template('index.html', **locals())

@app.route('/run', methods=['POST'])
@cross_origin()
def run():
    """
    start the simulation over the web application
    :return: 'OK'
    """
    start = pd.to_datetime(request.form['start'])
    end = pd.to_datetime(request.form['end'])
    print('starts simulation from %s to %s: ' % (start.date(), end.date()))
    simulation(start, end)
    return 'OK'

@app.route('/Grid', methods=['GET','POST'])
@cross_origin()
def grid():
    try:
        #TODO: Unterscheidung POST GET, damit try nicht benötigt wird
        #print(request.args)
        date = request.form['start']
        hour = int(request.form['hour'])
        print(date, '|', hour)
        fig = gridView.get_plot(date=pd.to_datetime(date), hour=hour)
        # fig = getPlot(date, hour)
        return render_template('tmp.html', plot=fig)
    except Exception as e:
        date = '2018-01-01'
        h = 1
        #fig = getPlot(date, hour)
        fig = None
        print('Error:',e)
        return render_template('grid.html', plot=fig)
    #fig = gridView.get_plot(date=pd.to_datetime('2018-01-01'), hour=1)
    #return render_template('grid.html', plot=fig)

@app.route('/build/start', methods=['POST'])
def buildAreas():
    """
    build up agents according to the setting of the webinterface
    :return: 'OK'
    """

    start = int(request.form['start'])              # start area
    end = int(request.form['end'])                  # end area

    for i in range(start, end + 1):

        if request.form['pwp'] == 'true':  # if true build PWP
            subprocess.Popen('python ' + path + r'/agents/pwp_Agent.py ' + '--plz %i'
                             % (i), cwd=path, shell=True)
        if request.form['res'] == 'true':  # if true build RES
            subprocess.Popen('python ' + path + r'/agents/res_Agent.py ' + '--plz %i'
                             % (i), cwd=path, shell=True)
        if request.form['dem'] == 'true':  # if true build DEM
            subprocess.Popen('python ' + path + r'/agents/dem_Agent.py ' + '--plz %i'
                             % (i), cwd=path, shell=True)
        if request.form['str'] == 'true':  # if true build STR
            subprocess.Popen('python ' + path + r'/agents/str_Agent.py ' + '--plz %i'
                             % (i), cwd=path, shell=True)

    if request.form['net'] == 'true':  # if true build STR
        pass
        #subprocess.Popen('python ' + path + r'/agents/net_Agent.py ' + '--plz %i'
        #                 % (i), cwd=path, shell=True)


    return 'OK'

"""
    simualtion method with command messagases for the agents
"""

def simulation(start, end, valid=True):

    influxCon.generate_weather(start - pd.DateOffset(days=1), end + pd.DateOffset(days=1), valid)

    if valid:
        print('write validation data')
        write_valid_data(database, 0, start, end)
        write_valid_data(database, 1, start, end)
        write_valid_data(database, 2, start, end)

    for date in pd.date_range(start=start, end=end, freq='D'):

        mongoCon.orderDB[str(date.date)]

        try:
            start_time = tme.time()
            send.basic_publish(exchange=exchange, routing_key='', body='opt_dayAhead ' + str(date))
            da_clearing(mongoCon, influxCon, date)
            send.basic_publish(exchange=exchange, routing_key='', body='grid_calc ' + str(date))
            send.basic_publish(exchange=exchange, routing_key='', body='result_dayAhead ' + str(date))
            print('Day Ahead calculation finish ' + str(date.date()))
            end_time = tme.time()-start_time
            print('Day complete in: %s seconds ' % end_time)

            # if valid:
            #     writeDayAheadError(database, date)
        except Exception as e:
            print('Error in Day Ahead calculation ' + str(date.date()))
            print(e)


if __name__ == "__main__":

    influxID, mongoID = -1, -1

    if config.getboolean('InfluxDB', 'Local'):  # if influxdb should runs local -> start
        influxPath = config['InfluxDB']['Path']
        cmd = subprocess.Popen([influxPath], shell=False, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                               stderr=subprocess.DEVNULL)
        influxID = cmd.pid

    if config.getboolean('MongoDB', 'Local'):   # if mongodb should runs local -> start
        mongoPath = config['MongoDB']['Path']
        cmd = subprocess.Popen([mongoPath, '-bind_ip', config['MongoDB']['Host']], shell=False,
                               stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
        mongoID = cmd.pid

    tme.sleep(1)                                 # wait one second till databases start up

    if config.getboolean('Results', 'Delete'):  # delete and clean up databases for new simulation
        # clean influxdb
        try:
            influxCon.influx.drop_database(database)
        except Exception as e:
            print(e)
        influxCon.influx.create_database(database)
        # clean mongodb
        for name in mongoCon.orderDB.list_collection_names():
            mongoCon.orderDB.drop_collection(name)

    try:
        if config.getboolean('Market', 'Local'): # if web application should runs local
            app.run(debug=False, port=marketPort, host='127.0.0.1')
        else:
            app.run(debug=False, port=marketPort, host=marketIp)
    except Exception as e:
        print(e)
    finally:
        send.basic_publish(exchange=exchange, routing_key='', body='kill ' + str('1970-01-01'))
        send.close()
        psutil.Process(influxID).kill()
        psutil.Process(mongoID).kill()
