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

| Physisch | DB Server | Simu-1 | Simu-2 |
| -- | -- | -- | -- |
| IPMI IP | 149.201.196.111 | 149.201.196.112 | 149.201.196.113 |
| IP 1 | 149.201.88.80 | 149.201.88.81 | 149.201.88.82 |
| IP 2 | NA | NA | NA |
| MAC IPMI | ac:1f:6b:e6:ee:35 | 3c:ec:ef:43:c8:f0 | 3c:ec:ef:43:c9:2a |
| MAC ETH 1 | ac:1f:6b:e6:f0:d2 | 3c:ec:ef:43:c6:dc | 3c:ec:ef:43:c7:50 |
| MAC ETH 2 | ac:1f:6b:e6:f0:d3 | 3c:ec:ef:43:c6:dd | 3c:ec:ef:43:c7:51 |
| DNS-Name | GO10S-NOWUM2 | GO10S-NOWUM3 | GO10S-NOWUM4 |
| OS | VMware ESXi 7.0 | VMware ESXi 7.0 | PROXMOX |

Virtuelle Server

| IP | MAC | Name | Funktion | Host |
| -- | -- | -- | -- | -- |
| 149.201.88.83 | 00:0c:29:aa:63:b0 | Influx_1 | Datenbank | GO10S-NOWUM2 |
| ~~149.201.196.107 | 5a:c2:1d:c7:70:b4 | ProxVM_1 | Simulation | GO10S-NOWUM4 ~~|


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


**Ports and Services:**

| Port | Service | purpose | Necessary where? | must reach |
| ------ | ------ | ------ | ------ | ------ |
| 22 | ssh | Maintaining/Control | everywhere | nothing |
| 80/443 | http/s | ??? | ??? | ??? |
| 3000 | Grafana | Dashboard / View only | once somewhere | InfluxDB, Web-App|
| 4200 | Angular | Frontend | once somewhere | Web-App |
| 5000 | Agent-Service | Waits for HTTP Post with instructions on agents to build | on any Server that should run Agents | InfluxDB, MongoDB, RabbitMQ |
| 5010 | Web-App | Control Simulation | once somewhere | RabbitMQ, Agent-Service |
| 5672 | RabbitMQ | MQT Msgs | ??? | ??? |
| 5673 | RabbitMQ | MQT Msgs | ??? | ??? |
| 8006 | Proxmox Webinterface | Server Configuration | only for admin | nothing |
| 8086 | InfluxDB (Running)| Time Series Database for Weather and Agents | once somewhere | nothing, only be reachable |
| 8088 | InfluxDB (Backup) | only needed to backup Database | once somewhre | 
| 15672 | RabbitMQ | Communication between Agents and Market | once somewhere | nothing, must be reachable for web-app and agents |
| 27017 | MongoDB | master data(Stammdaten) and bids from agents | once somewhere | nothing, must be reachable for agents |


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

> export GUROBI_HOME="/usr/local/VERSION/linux64" <br>
> export PATH="${PATH}:${GUROBI_HOME}/bin" <br>
> export LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:${GUROBI_HOME}/lib" <br>
> export PYTHONPATH="$HOME/dmas/model" <br>
> export GRB_LICENSE_FILE="$HOME/gurobi.lic" <br>

add the export lines to the end

**Refresh your Bash:**<br>

7. `source /etc/bash.bashrc` <br>

**Register Gurobi with key (once for every user):**<br>

8. `grbgetkey KEY` save to your home directory <br>

**Test if gurobi and links are working**

9. `gurobi_cl`

# Important Commands
- kill all python tasks on windows:  `taskkill /F /IM python.exe /T /fi "USERNAME eq Rieke"` <br>
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




