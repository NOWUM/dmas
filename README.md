# Linux Server
Requirements:

- Python 3.5 or newer includes:
    - pandas
    - requests
    - numba
    - tensorflow
    - influxDB
    - pika
    - pymongo
    - geohash2
    - sklearn
    - keras
    - pyswarm
    - xlrd

Install python modules
$ pip3 install -r requirements.txt

- Gurobi 8 or newer


# Install Gurobi in /usr/local
$ wget http://packages.gurobi.com/VERSION.tar.gz
$ tar xzvf gurobi9.0.1_linux64.tar.gz
$ cd gurobi_VERSION/linux64
$ sudo python3 setup.py install (needs to be done for every user)

register gurobi with key
$ grbgetkey KEY
choose /opt/gurobi as file location, create if directory doesn't exist or copy file later if you can only create it in your home directory
$ export $GRB_LICENSE_FILE=/opt/gurobi

Add Gurobi Variables Globally:
nano /etc/bash.bashrc (add to the end)

export GUROBI_HOME="/usr/local/gurobi901/linux64"
export PATH="${PATH}:${GUROBI_HOME}/bin"
export LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:${GUROBI_HOME}/lib"

Test by calling
$ gurobi_cl

Add Pythonpath for your user (Only till logout - find permanent solution )
export PYTHONPATH="home/%USERNAME/MAS"


# Bash-Screen 
- Informationen:      https://wiki.ubuntuusers.de/Screen/ 
- Screen beenden:     screen -X -S NAME quit
- Alle beenden:       sudo pkill -2 python
                      sudo pkill screen
