import configparser
import os
import subprocess
import time as tm

import pandas as pd
import pika
import psutil
from apps.routine_Balancing import balPowerClearing, balEnergyClearing
from apps.routine_DayAhead import dayAheadClearing
from flask import Flask, render_template, request
from flask_cors import cross_origin
from interfaces.interface_Influx import influxInterface as influxCon
from interfaces.interface_mongo import mongoInterface as mongoCon

config = configparser.ConfigParser()
config.read('app.cfg')

database = config['Results']['Database']
mongoCon = mongoCon(host=config['MongoDB']['host'], database=database)
influxCon = influxCon(host=config['InfluxDB']['host'], database=database)

credentials = pika.PlainCredentials('dMAS', 'dMAS2020')
connection = pika.BlockingConnection(pika.ConnectionParameters(host=config['Market']['host'],
                                                               heartbeat=0, credentials=credentials))
send = connection.channel()
send.exchange_declare(exchange='Market', exchange_type='fanout')

app = Flask(__name__)
path = os.path.dirname(os.path.dirname(__file__)) + r'/model'
pd.set_option('mode.chained_assignment', None)


# ----- Starting View -----
@app.route('/')
def index():
    num = []
    agent_ids = mongoCon.status.find().distinct('_id')
    for typ in ['PWP', 'RES', 'DEM']:
        counter = 0
        for id in agent_ids:
            if typ == id.split('_')[0]:
                counter += 1
        num.append(counter)
    return render_template('index.html', **locals())


# ----- Start Simulation -----
@app.route('/run', methods=['POST'])
@cross_origin()
def run():
    start = pd.to_datetime(request.form['start'])
    end = pd.to_datetime(request.form['end'])
    print('starte  Simulation von %s bis %s: ' % (start.date(), end.date()))
    simulation(start, end)
    return 'OK'


# ----- Start Areas -----
@app.route('/build/start', methods=['POST'])
def buildAreas():
    # -- Start and End Area
    start = int(request.form['start'])
    end = int(request.form['end'])

    for i in range(start, end + 1):
        influx = config['InfluxDB']['host']
        mongo = config['MongoDB']['host']
        market = config['Market']['host']
        database = config['Results']['Database']

        if request.form['res'] == 'true':  # -- if true build RES
            subprocess.Popen('python ' + path + r'/agents/res_Agent.py ' + '--plz %i --mongo %s --influx %s --market %s --dbName %s'
                             % (i, mongo, influx, market, database), cwd=path, shell=True)
        if request.form['dem'] == 'true':  # -- if true build DEM
            subprocess.Popen('python ' + path + r'/agents/dem_Agent.py ' + '--plz %i --mongo %s --influx %s --market %s --dbName %s'
                             % (i, mongo, influx, market, database), cwd=path, shell=True)
        if request.form['pwp'] == 'true':  # -- if true build PWP
            subprocess.Popen('python ' + path + r'/agents/pwp_Agent.py ' + '--plz %i --mongo %s --influx %s --market %s --dbName %s'
                             % (i, mongo, influx, market, database), cwd=path, shell=True)
    return 'OK'


# ----- Simulation Task -----
def simulation(start, end, valid=True):
    influxCon.generateWeather(start, end  + pd.DateOffset(days=1), valid)

    for date in pd.date_range(start=start, end=end, freq='D'):

        mongoCon.orderDB[str(date.date)]

        try:
            #send.basic_publish(exchange='Market', routing_key='', body='opt_balancing ' + str(date))
            #balPowerClearing(mongoCon, influxCon, date)
            #send.basic_publish(exchange='Market', routing_key='', body='result_balancing ' + str(date))
            print('Balancing calculation finish ' + str(date.date()))
        except Exception as e:
            print('Error in  Balancing calculation ' + str(date.date()))
            print(e)
        try:
            send.basic_publish(exchange='Market', routing_key='', body='opt_dayAhead ' + str(date))
            dayAheadClearing(mongoCon, influxCon, date)
            send.basic_publish(exchange='Market', routing_key='', body='result_dayAhead ' + str(date))
            print('Day Ahead calculation finish ' + str(date.date()))
        except Exception as e:
            print('Error in Day Ahead calculation ' + str(date.date()))
            print(e)
        try:
            #send.basic_publish(exchange='Market', routing_key='', body='opt_actual ' + str(date))
            #balEnergyClearing(mongoCon, influxCon, date)
            send.basic_publish(exchange='Market', routing_key='', body='result_actual ' + str(date))
            print('Actual calculation finish ' + str(date.date()))
        except Exception as e:
            print('Error in Actual ' + str(date.date()))
            print(e)


if __name__ == "__main__":
    pids = []
    # ----- InfluxDB -----
    if config.getboolean('InfluxDB', 'Local'):
        influxPath = config['InfluxDB']['Path']
        cmd = subprocess.Popen([influxPath], shell=False, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                               stderr=subprocess.DEVNULL)
        pids.append(cmd.pid)
    if config.getboolean('MongoDB', 'Local'):
        # ----- MongoDB -----
        mongoPath = config['MongoDB']['Path']
        cmd = subprocess.Popen([mongoPath, '-bind_ip', config['MongoDB']['host']], shell=False,
                               stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
        pids.append(cmd.pid)

    tm.sleep(2)

    try:
        influxCon.influx.drop_database(database)
    except:
        pass
    influxCon.influx.create_database(database)
    for name in mongoCon.orderDB.list_collection_names():
        mongoCon.orderDB.drop_collection(name)

    init = mongoCon.orderDB["init"]
    query = {"_id": 'start'}
    start = {"$set": {"_id": "start", "start": tm.ctime(), "config": config}}
    init.update_one(filter=query, update=start, upsert=True)

    try:
        app.run(debug=False, port=5010, host='0.0.0.0')
    except Exception as e:
        print(e)
    finally:
        send.basic_publish(exchange='Market', routing_key='', body='kill ' + str('1970-01-01'))
        send.close()
        tm.sleep(2)
        for pid in pids:
            try:
                psutil.Process(pid).kill()
            except:
                pass
