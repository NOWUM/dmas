# third party modules
import configparser
import subprocess
import os
from flask import Flask, render_template, request
from flask_cors import CORS
import socket


hostname = socket.gethostname()
ip_address = socket.gethostbyname(hostname)

app = Flask(__name__)
cors = CORS(app, resources={r"*": {"origins": "*"}})
config = configparser.ConfigParser()
config.read('agent.cfg')
path = os.path.dirname(os.path.dirname(__file__)) + r'/model'


@app.route('/', methods=['GET'])
def status():
    return render_template('agent.html')


@app.route('/test', methods=['GET'])
def test():
    return 'OK'


@app.route('/config', methods=['GET', 'POST'])
def set_config():
    content = request.json
    for key, value in content.items():
        config['Server'][key] = value

    with open('agent.cfg', 'w') as configfile:
        config.write(configfile)

    return 'OK'


@app.route('/build', methods=['GET', 'POST'])
def build():

    content = request.json
    if os.name == 'nt':
        for i in range(int(content['start']), int(content['end'])):
            command = 'python ' + path + r'/agents/' + content['typ'] + '_Agent.py ' + '--plz ' + str(i)
            subprocess.Popen(command, cwd=path, shell=True)
    else:
        for i in range(int(content['start']), int(content['end'])):
            command = 'python3 ' + path + r'/agents/' + content['typ'] + '_Agent.py ' + '--plz ' + str(i)
            subprocess.Popen(command, cwd=path, shell=True)

    return 'OK'


if __name__ == "__main__":

    app.run(debug=False, port=5000, host=ip_address)


