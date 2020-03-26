import pandas as pd
import  os
from flask import Flask, render_template, request,jsonify
from flask_cors import cross_origin
import pika
import subprocess
from interfaces.interface_sqlite import sqliteInterface as sqliteCon
from interfaces.interface_Influx import influxInterface as influxCon
from apps.routine_DayAhead import dayAheadClearing
from apps.routine_Balancing import balPowerClearing, balEnergyClearing
import time as tm
import configparser


config = configparser.ConfigParser()
config.read('app.cfg')

sqliteCon = sqliteCon()
sqliteCon.createTables()
influxCon = influxCon(config['InfluxDB']['host'])

connection = pika.BlockingConnection(pika.ConnectionParameters(host=config['Market']['host'], heartbeat=0))
send = connection.channel()
send.exchange_declare(exchange='Market', exchange_type='fanout')

app = Flask(__name__)
path = os.path.dirname(os.path.dirname(__file__)) + r'/model'
pd.set_option('mode.chained_assignment', None)

# ----- Starting View -----
@app.route('/')
def index():
    num = []
    for typ in ['PWP', 'RES', 'DEM']:
        agents = sqliteCon.getNumberAgentTyps(typ)
        num.append(len(agents))
    return render_template('index.html', **locals())

# ----- Start Simulation -----
@app.route('/run', methods=['POST'])
@cross_origin()
def run():
    print('start Simulation: ')
    start = pd.to_datetime(request.form['start'])
    end = pd.to_datetime(request.form['end'])
    print(' --> from: %s' %start)
    print(' --> to: %s' %end)
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

        if request.form['res'] == 'true':       # -- if true build RES
            subprocess.Popen('python ' + path + r'/agents/res_Agent.py ' + '--plz %i --mongo %s --influx %s --market %s'
                             %(i, mongo, influx, market), cwd=path, shell=True)
        if request.form['dem'] == 'true':       # -- if true build DEM
            subprocess.Popen('python ' + path + r'/agents/dem_Agent.py ' + '--plz %i --mongo %s --influx %s --market %s'
                             %(i, mongo, influx, market), cwd=path, shell=True)
        if request.form['pwp']  == 'true':      # -- if true build PWP
            subprocess.Popen('python ' + path + r'/agents/pwp_Agent.py ' + '--plz %i --mongo %s --influx %s --market %s'
                             %(i, mongo, influx, market), cwd=path, shell=True)
    return 'OK'

# ----- Day Ahead Orders -----
@app.route('/orders', methods=['POST'])
def set_ordersDA():
    content = request.json
    sqliteCon.setDayAhead(request)
    return jsonify({"uuid": content['uuid']})

#----- Balancing Orders -----
@app.route('/balancing', methods=['POST'])
def set_balancing():
    content = request.json
    sqliteCon.setBalancing(content)
    return jsonify({"uuid": content['uuid']})

# ----- Actuals Schedule -----
@app.route('/actuals',methods=['POST'])
def set_actuals():
    content = request.json
    sqliteCon.setActuals(request)
    return jsonify({"uuid": content['uuid']})

# ----- Login -----
@app.route('/login',methods=['POST'])
def login():
    content = request.json
    print(content['uuid'])
    sqliteCon.loginAgent(request)
    return jsonify({"uuid": content['uuid']})

# ----- Agent Logout -----
@app.route('/logout',methods=['POST'])
def logout():
    content = request.json
    sqliteCon.logoutAgent(request)
    return jsonify({"uuid": content['uuid']})

# ----- Simulation Task -----
def simulation(start, end):

    influxCon.generateWeather(start, end)

    for date in pd.date_range(start=start,end=end,freq='D'):
        try:
            send.basic_publish(exchange='Market', routing_key='', body='opt_balancing ' + str(date))
            balPowerClearing(sqliteCon, influxCon, date)
            send.basic_publish(exchange='Market', routing_key='', body='result_balancing ' + str(date))
            print('Balancing calculation finish ' + str(date.date()))
        except Exception as e:
            print('Error in  Balancing calculation ' + str(date.date()))
            print(e)
            sqliteCon.deleteBalancing()
        try:
            send.basic_publish(exchange='Market', routing_key='', body='opt_dayAhead ' + str(date))
            dayAheadClearing(sqliteCon, influxCon, date)
            send.basic_publish(exchange='Market', routing_key='', body='result_dayAhead ' + str(date))
            print('Day Ahead calculation finish ' + str(date.date()))
            sqliteCon.deleteDayAhead()
        except Exception as e:
            print('Error in Day Ahead calculation ' + str(date.date()))
            print(e)
            sqliteCon.deleteDayAhead()
        try:
            send.basic_publish(exchange='Market', routing_key='', body='opt_actual ' + str(date))
            balEnergyClearing(sqliteCon, influxCon, date)
            send.basic_publish(exchange='Market', routing_key='', body='result_actual ' + str(date))
            print('Actual calculation finish ' + str(date.date()))
            sqliteCon.deleteBalancing()
            sqliteCon.deleteActuals()
        except Exception as e:
            print('Error in Actual ' + str(date.date()))
            print(e)
            sqliteCon.deleteActuals()
            sqliteCon.deleteBalancing()

if __name__ == "__main__":
    # ----- InfluxDB -----
    if config.getboolean('InfluxDB','Local'):
        influxPath = config['InfluxDB']['Path']
        cmd = subprocess.Popen([influxPath], shell=False, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        sqliteCon.startServices(name='influx', pid=cmd.pid)
    if config.getboolean('MongoDB', 'Local'):
    # ----- MongoDB -----
        mongoPath = config['MongoDB']['Path']
        cmd = subprocess.Popen([mongoPath, '-bind_ip', config['MongoDB']['host']], shell=False, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
        sqliteCon.startServices(name='mongo', pid=cmd.pid)

    tm.sleep(2)
    influxCon.influx.drop_database('MAS_2019')
    influxCon.influx.create_database('MAS_2019')

    try:
        app.run(debug=False, port=5010, host='0.0.0.0')
    except Exception as e:
        print(e)
    finally:
        send.basic_publish(exchange='Market', routing_key='', body='kill ' + str('1970-01-01'))
        send.close()
        tm.sleep(2)
        sqliteCon.stopServices()

