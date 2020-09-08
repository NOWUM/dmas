# Requirements

**Software**

| Programm | Link |
| ------ | ------ |
| Python 3.7 / 3.8           | / |
| Gurobi 9.0                | / |
| Grafana 6.4               | https://grafana.com/grafana/download |
| RabbitMQ 3.8              | https://www.rabbitmq.com/download.html |
| InfluxDB 1.5.4            | https://www.influxdata.com/get-influxdb/ |
| MongoDB                   | https://www.mongodb.com/download-center/community |
| Erlang Server             | https://www.rabbitmq.com/which-erlang.html |

install python modules: `pip3 install -r requirements.txt`


**Hardware**

**Agents:**
- pwp (conventionell power plants) 250 MB RAM per Agent
- res (renewable energy systems) 400 MB RAM per Agent
- dem (demand systems) 300 MB RAM per Agent
- net (grid operation) unkown

**Germany:**
- pwp ~ 25 GB RAM 
- res ~ 40 GB RAM 
- dem ~ 30 GB RAM 
- net ~ unkown

**Others:**
- InfluxDB ~ unkown
- Clearing ~ unkown
- Misc ~ unkown



# Install Gurobi on Linux System

**Get and Istall Gurobi** <br>
1. `cd /usr/local/` <br>
2. `sudo wget http://packages.gurobi.com/VERSION.tar.gz` <br>
    Source: https://packages.gurobi.com/9.0/gurobi9.0.1_linux64.tar.gz <br>
    Command:  `sudo wget https://packages.gurobi.com/9.0/gurobi9.0.1_linux64.tar.gz`
3. `sudo tar xzvf gurobi9.0.1_linux64.tar.gz` <br>
4. `cd gurobi_VERSION/linux64` <br>
5. `sudo python3 setup.py install` <br>


**Add Gurobi Variables Globally:**<br>

6. `sudo nano /etc/bash.bashrc` <br>

> export GUROBI_HOME="/usr/local/gurobi901/linux64" <br>
> export PATH="${PATH}:${GUROBI_HOME}/bin" <br>
> export LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:${GUROBI_HOME}/lib" <br>
> export PYTHONPATH="$HOME/dmas/model" <br>
> export GRB_LICENSE_FILE="$HOME/gurobi.lic" <br>

add the export lines to the end

**Refresh your Bash:**<br>

7. `source /etc/bash.bashrc` <br>

**Register Gurobi with key (once for every user):**<br>

8. `sudo grbgetkey KEY` save to your home directory <br>

**Test if gurobi and links are working**

9. `gurobi_cl`

# Important Commands
- kill all python tasks on windows:  `taskkill /F /IM pytho.exe /T /fi "USERNAME eq Rieke"` <br>
- kill screen session on linux: `sudo screen -X -S NAME quit`<br>
- kill all screens on linux:   `sudo pkill screen`<br>
- kill all python tasks on linux: `sudo pkill -2 python3`<br>



# FH specific Addresses and Endpoints (can vary)

**InfluxDB**<br>
Host = 149.201.88.150<br>
Port = 8086<br>
Local = True<br>
Path = C:\Program Files\SimEnv\database\influxdb\influxd.exe<br>

**MongoDB**<br>
Host = 149.201.88.150<br>
Port = 27017<br>
Local = True<br>
Path = C:\Program Files\SimEnv\database\mongodb\bin\mongod.exe<br>

**Market**<br>
Host = 149.201.88.150<br>
Port = 5010<br>
Local = False<br>
Exchange = Market<br>
Agentsuffix =<br>

**Results**<br>
Database = MAS2020_8<br>
Delete = True<br>




