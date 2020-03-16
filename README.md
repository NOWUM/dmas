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

Install python modules (requirements.txt in /home/flo/MAS/)
$ pip3 install -r requirements.txt

- Gurobi 8 or newer


# Install and Add Gurobi to Path
$ wget http://packages.gurobi.com/VERSION.tar.gz
$ tar xzvf gurobi9.0.1_linux64.tar.gz
$ cd gurobi_VERSION/linux64
$ sudo python3 setup.py install (needs to be done for every user)

Add Gurobi to PATH (execute for every user):
nano ~/.bashrc (add to the end)

export GUROBI_HOME="/home/flo/MAS/gurobi901/linux64"
export PATH="${PATH}:${GUROBI_HOME}/bin"
export LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:${GUROBI_HOME}/lib"

and for Python Path finding
export PYTHONPATH="home/rieke/MAS" 
or the Path to your installation

register gurobi with key
$ grbgetkey KEY

# Bash-Screen 
Informationen:      https://wiki.ubuntuusers.de/Screen/ 
Screen beenden:     screen -X -S NAME quit
Alle beenden:       sudo pkill -2 python
                    sudo pkill screen
