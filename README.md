# Windows Server
**Agents Requirements:** 
- Python 3.5 or newer includes: pandas, requests, numba, tensorflow, influxDB, 
                                pika, pymongo, geohash2, sklearn, keras, pyswarm,
                                xlrd
- Gurobi 8 or newer

**Plattform Requirements:**
- Python 3.5 or newer includes: pandas, requests, influxDB, pymongo, xlrd, flask, flask_cors, pika
- InfluxDB v1.5 or newer (https://www.influxdata.com/get-influxdb/)
- MongoDB (https://www.mongodb.com/download-center/community)
- SQLite (https://www.sqlitetutorial.net/sqlite-python/)
- Grafana v 6.4 or newer (https://grafana.com/grafana/download)
- RabbitMQ (https://www.rabbitmq.com/download.html)
- Erlang Server (see RabbitMQ Introduction // https://www.rabbitmq.com/which-erlang.html) 

**Install python modules:** <br>
$ pip install -r requirements.txt

**Install Gurobi** <br>
ToDo

**Wichtige Befehle** <br>
taskkill /F /IM python.exe /T /fi "USERNAME eq Rieke"

# Linux Server
**Agents Requirements:**
- Python 3.5 or newer includes: pandas, requests, numba, tensorflow, influxDB, 
                                pika, pymongo, geohash2, sklearn, keras, pyswarm,
                                xlrd
- Gurobi 8 or newer

**Plattform Requirements:**
- Python 3.5 or newer includes: pandas, requests, influxDB, pymongo, xlrd, flask, flask_cors, pika
- InfluxDB v1.5 or newer (https://www.influxdata.com/get-influxdb/)
- MongoDB (https://www.mongodb.com/download-center/community)
- SQLite (https://www.sqlitetutorial.net/sqlite-python/)
- Grafana v 6.4 or newer (https://grafana.com/grafana/download)
- RabbitMQ (https://www.rabbitmq.com/download.html)
- Erlang Server (see RabbitMQ Introduction // https://www.rabbitmq.com/which-erlang.html) 

**Install python modules:** <br>
$ pip3 install -r requirements.txt

**Install Gurobi in /usr/local** <br>
$ sudo wget http://packages.gurobi.com/VERSION.tar.gz <br>
(https://packages.gurobi.com/9.0/gurobi9.0.1_linux64.tar.gz) 
Bsp: $ sudo wget https://packages.gurobi.com/9.0/gurobi9.0.1_linux64.tar.gz <br>
$ sudo tar xzvf gurobi9.0.1_linux64.tar.gz <br>
$ cd gurobi_VERSION/linux64 <br>
$ sudo python3 setup.py install <br>

**Add Gurobi Variables Globally:**<br>
$ nano /etc/bash.bashrc (add the export lines to the end) <br>
export GUROBI_HOME="/usr/local/gurobi901/linux64" <br>
export PATH="${PATH}:${GUROBI_HOME}/bin" <br>
export LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:${GUROBI_HOME}/lib" <br>
export $GRB_LICENSE_FILE=/opt/gurobi/gurobi.lic <br>
source /etc/bash.bashrc <br>

**Register gurobi with key**<br>
$ grbgetkey KEY <br>
choose /opt/gurobi as file location, create if directory doesn't exist or copy file later if you can only create it in your home directory <br>

Test if gurobi and links are working by calling $ gurobi_cl <br>


**Add Pythonpath for your user [Needs to be done for every user, TODO add dynamic path adding to /etc/bash.bashrc]**  <br>
nano .profile <br>
export PYTHONPATH="home/%USERNAME/dmas/model" <br>

**Bash-Screen** <br>
Informationen: [Wiki](https://wiki.ubuntuusers.de/Screen/ ) <br> <br>
Wichtige Befehle:
*  Screen beenden:     $ screen -X -S NAME quit
*  Alle beenden:       $ sudo pkill -2 python + $ sudo pkill screen 
