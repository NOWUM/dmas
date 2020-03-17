# Windows Server
**Requirements:** 
- Python 3.5 or newer includes: pandas, requests, numba, tensorflow, influxDB, 
                                pika, pymongo, geohash2, sklearn, keras, pyswarm,
                                xlrd
- Gurobi 8 or newer

**Install python modules:** <br>
$ pip install -r requirements.txt

**Install Gurobi** <br>
ToDo


# Linux Server
**Requirements:**
- Python 3.5 or newer includes: pandas, requests, numba, tensorflow, influxDB, 
                                pika, pymongo, geohash2, sklearn, keras, pyswarm,
                                xlrd
- Gurobi 8 or newer

**Install python modules:** <br>
$ pip3 install -r requirements.txt

**Install Gurobi in /usr/local** <br>
$ wget http://packages.gurobi.com/VERSION.tar.gz (gurobi9.0.1_linux64.tar.gz) <br>
$ tar xzvf gurobi9.0.1_linux64.tar.gz <br>
$ cd gurobi_VERSION/linux64 <br>
$ sudo python3 setup.py install (needs to be done for every user) <br>

**Register gurobi with key**<br>
$ grbgetkey KEY <br>
choose /opt/gurobi as file location, create if directory doesn't exist or copy file later if you can only create it in your home directory <br>
$ export $GRB_LICENSE_FILE=/opt/gurobi <br>

**Add Gurobi Variables Globally:**<br>
$ nano /etc/bash.bashrc (add to the end) <br>
export GUROBI_HOME="/usr/local/gurobi901/linux64" <br>
export PATH="${PATH}:${GUROBI_HOME}/bin" <br>
export LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:${GUROBI_HOME}/lib" <br>
Test by calling $ gurobi_cl <br>

**Add Pythonpath for your user**  <br>
export PYTHONPATH="home/%USERNAME/MAS" <br>
ToDo: (Only till logout - find permanent solution )

**Bash-Screen** <br>
Informationen: [Wiki](https://wiki.ubuntuusers.de/Screen/ ) <br> <br>
Wichtige Befehle:
*  Screen beenden:     $ screen -X -S NAME quit
*  Alle beenden:       $ sudo pkill -2 python + $ sudo pkill screen 
