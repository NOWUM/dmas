#!/usr/bin/python3
import json
import sys

image_repo = 'registry.git.fh-aachen.de/nowum-energy/projects/dmas/'
counter = int(sys.argv[1])
if counter > 100:
    max_connections = 5000
    structure_servers = ['10.13.10.54:4321', '10.13.10.55:4321', '10.13.10.56:4321', '10.13.10.58:4321', '10.13.10.59:4321']
else:
    max_connections = 500
    structure_servers = ['10.13.10.41:5432']

NUTS_LEVEL = int(sys.argv[2]) # which aggregation should we use?
NET = False
ENTSOE = False # should we take the actual ENTSO-E demand or oep data?

structure_index = 0


def structure_server():
    global structure_index
    idx = structure_index % len(structure_servers)
    structure_index += 1
    return structure_servers[idx]

# Build Demand Agents
with open('./agents.json', 'r') as f:
    raw_agents = json.load(f)

agents = {}
for agent_type, agent_list in raw_agents.items():
    agents[agent_type] = list({a[0:2+NUTS_LEVEL] for a in agent_list})

configs = {}
output = []
output.append('version: "3.9"\n')
output.append('services:\n')

output.append('''
  # gurobi-compute with wls-license
  compute:
    container_name: gurobi-compute
    image: gurobi/compute:latest
    volumes:
      - /sys/fs/cgroup:/sys/fs/cgroup:rw
    configs:
      - source: compute_config
        target: /opt/gurobi/gurobi.lic
    deploy:
      mode: replicated
      replicas: 1
      placement:
        constraints: [node.role == manager]
''')
configs['compute_config'] = './gurobi.lic'

output.append(f'''
  simulationdb:
    container_name: simulationdb
    image: timescale/timescaledb:latest-pg16
    #command: postgres -c 'max_connections=500' -B 4096MB
    restart: always
    environment:
      - POSTGRES_USER=dMAS
      - POSTGRES_PASSWORD=dMAS
      - POSTGRES_DB=dmas
      - TS_TUNE_MAX_CONNS={max_connections}
    ports:
      - 5433:5432
    volumes:
        - ./init.sql:/docker-entrypoint-initdb.d/init.sql
    deploy:
      mode: replicated
      replicas: 1
      placement:
        constraints: [node.role == manager]

  grafana:
    image: grafana/grafana
    container_name: dmas-grafana
    user: "104"
    depends_on:
      - simulationdb
    ports:
      - 3001:3000
    environment:
      GF_SECURITY_ALLOW_EMBEDDING: "true"
      GF_AUTH_ANONYMOUS_ENABLED: "true"
      GF_INSTALL_PLUGINS: volkovlabs-echarts-panel
    volumes:
      - ./grafana/datasources:/etc/grafana/provisioning/datasources
      - ./grafana/dashboards:/etc/grafana/provisioning/dashboards
      - ./grafana/dashboard-definitions:/etc/grafana/provisioning/dashboard-definitions
    restart: always
    deploy:
      mode: replicated
      replicas: 1
      placement:
        constraints: [node.role == manager]

''')
# Build one Control Agent
output.append(f'''
  controller:
    container_name: controller
    image: {image_repo}agent:latest
    build: .
    environment:
      AREA_CODE: DE111
      TYPE: 'CTL'
      SIMULATION_SOURCE: 'simulationdb:5432'
      WS_HOST: '0.0.0.0'
    ports:
      - 5000:5000
      - 4000:4000
    deploy:
      mode: replicated
      replicas: 1
      placement:
        constraints: [node.role == manager]
    depends_on:
      - simulationdb
''')
# Build one Market
output.append(f'''
  market:
    container_name: market
    image: {image_repo}agent:latest
    environment:
      AREA_CODE: DE111
      TYPE: 'MRK'
      SIMULATION_SOURCE: 'simulationdb:5432'
      WS_HOST: 'controller'
    configs:
      - source: market_config
        target: /opt/gurobi/gurobi.lic
    depends_on:
      - controller

''')
configs['market_config']= './market_gurobi.lic'

if NET:
  # Build one TSO
  output.append(f'''
    net:
      container_name: net
      image: {image_repo}agent:latest
      environment:
        AREA_CODE: DE111
        TYPE: 'NET'
        SIMULATION_SOURCE: 'simulationdb:5432'
        WS_HOST: 'controller'
      depends_on:
        - controller
  ''')
# Build Demand Agents
if ENTSOE:
    output.append(f'''
  dem:
    container_name: dem
    image: {image_repo}agent:latest
    environment:
      AREA_CODE: DE111
      TYPE: 'DEO'
      SIMULATION_SOURCE: 'simulationdb:5432'
      WS_HOST: 'controller'
    depends_on:
      - controller
''')
else:
  for agent in agents['dem'][:counter]:
      output.append(f'''
  dem_{agent.lower()}:
    container_name: dem_{agent.lower()}
    image: {image_repo}agent:latest
    environment:
      AREA_CODE: {agent}
      TYPE: 'DEM'
      SIMULATION_SOURCE: 'simulationdb:5432'
      STRUCTURE_SERVER: '{structure_server()}'
      WS_HOST: 'controller'
    depends_on:
      - controller
''')
# Build Power Plant Agents
for agent in agents['pwp'][:counter]:
    output.append(f'''
  pwp_{agent.lower()}:
    container_name: pwp_{agent.lower()}
    image: {image_repo}agent:latest
    environment:
      AREA_CODE: {agent}
      TYPE: 'PWP'
      SIMULATION_SOURCE: 'simulationdb:5432'
      WS_HOST: 'controller'
      STRUCTURE_SERVER: '{structure_server()}'
      REAL_PRICES: 'True'
''')
# Build Renewable Energy Agents
for agent in agents['res'][:counter]:
    output.append(f'''
  res_{agent.lower()}:
    container_name: res_{agent.lower()}
    image: {image_repo}agent:latest
    environment:
      AREA_CODE: {agent}
      TYPE: 'RES'
      SIMULATION_SOURCE: 'simulationdb:5432'
      WS_HOST: 'controller'
      STRUCTURE_SERVER: '{structure_server()}'
    depends_on:
      - controller
''')
# Build Storage Agents
for agent in agents['str'][:counter]:
    output.append(f'''
  str_{agent.lower()}:
    container_name: str_{agent.lower()}
    image: {image_repo}agent:latest
    environment:
      AREA_CODE: {agent}
      TYPE: 'STR'
      SIMULATION_SOURCE: 'simulationdb:5432'
      WS_HOST: 'controller'
      STRUCTURE_SERVER: '{structure_server()}'
      REAL_PRICES: 'True'
    depends_on:
      - controller
''')
output.append('configs:')
for config, location in configs.items():
    output.append(f'''
  {config}:
    file: {location}
''')
with open('compose.yml', 'w') as f:
    f.writelines(output)
