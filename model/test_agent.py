import requests
import os
import configparser


if __name__ == "__main__":
    path = os.path.dirname(os.path.dirname(__file__)) + r'/model'    # change directory
    config = configparser.ConfigParser()                             # read config file
    config.read('app.cfg')

    agent_conf = {}
    for key, value in config.items():
        if 'Agent' in key:
            dict_ = {}
            for k, v in value.items():
                dict_.update({k: v})
            x = key.split('-')[0]
            agent_conf.update({x.lower(): dict_})

    #database = config['Results']['database']
    #influx_ip = config['InfluxDB']['host']
    #mongo_ip = config['MongoDB']['host']
    #mqtt_ip = config['RabbitMQ']['host']
    #exchange = config['RabbitMQ']['exchange']

    #myJSON = {'database': database, 'influx': influx_ip, 'mongo': mongo_ip, 'mqtt': mqtt_ip, 'exchange': exchange}
    #res = requests.post('http://149.201.88.150:5010/config', json=myJSON)

