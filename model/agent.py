# third party modules
import configparser
import subprocess
import os
from flask import Flask, render_template, request
from flask_cors import CORS, cross_origin


app = Flask(__name__)
cors = CORS(app, resources={r"/*": {"origins": "*"}})
config = configparser.ConfigParser()
config.read('agent.cfg')
path = os.path.dirname(os.path.dirname(__file__)) + r'/model'


@app.route('/', methods=['GET'])
@cross_origin()
def status():
    return render_template('agent.html')


@app.route('/config', methods=['GET', 'POST'])
@cross_origin()
def set_config():
    content = request.json
    for key, value in content.items():
        config['Server'][key] = value

    with open('agent.cfg', 'w') as configfile:
        config.write(configfile)

    return 'OK'


@app.route('/build', methods=['GET', 'POST'])
@cross_origin()
def build():

    content = request.json
    print(os.name)
    if os.name == 'nt':
        print(content['typ'])
        for i in range(int(content['start']), int(content['end'] + 1)):
            command = 'python ' + path + r'/agents/' + content['typ'] + '_Agent.py ' + '--plz ' + str(i)
            subprocess.Popen(command, cwd=path, shell=True)
    else:
        for i in range(int(content['start']), int(content['end'] + 1)):
            command = 'python3 ' + path + r'/agents/' + content['typ'] + '_Agent.py ' + '--plz ' + str(i)
            subprocess.Popen(command, cwd=path, shell=True)

    return 'OK'


if __name__ == "__main__":

    app.run(debug=False, port=5000, host='127.0.0.1')


