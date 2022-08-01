#!/usr/bin/python3
import numpy as np

image_repo = 'registry.git.fh-aachen.de/nowum-energy/projects/dmas/'
counter = 1

configs = {}
output = []
output.append('version: "3.9"\n')
output.append('services:\n')

output.append(f'''
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
configs[f'compute_config']= f'./gurobi.lic'

output.append(f'''
  simulationdb:
    container_name: simulationdb
    image: timescale/timescaledb:latest-pg14
    #command: postgres -c 'max_connections=500' -B 4096MB
    restart: always
    environment:
      - POSTGRES_USER=dMAS
      - POSTGRES_PASSWORD=dMAS
      - POSTGRES_DB=dmas
      - TS_TUNE_MAX_CONNS=2000
    ports:
      - 5432:5432
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
# Build Rabbitmq
output.append('''
  rabbitmq:
    container_name: rabbitmq
    image: rabbitmq:3-management
    restart: always
    ports:
      - 15672:15672
      - 5672:5672
    deploy:
      mode: replicated
      replicas: 1
      placement:
        constraints: [node.role == manager]
''')
# Build one Control Agent
output.append(f'''
  controller:
    container_name: ctl
    image: {image_repo}agent:latest
    build: .
    environment:
      AREA_CODE: 'DE111'
      TYPE: 'CTL'
      MQTT_HOST: 'rabbitmq'
      SIMULATION_SOURCE: 'simulationdb:5432'
      WS_HOST: '0.0.0.0'
    ports:
      - 5000:5000
    deploy:
      mode: replicated
      replicas: 1
      placement:
        constraints: [node.role == manager]
''')
# Build one Market
output.append(f'''
  market:
    container_name: mrk
    image: {image_repo}agent:latest
    environment:
      AREA_CODE: 'DE111'
      TYPE: 'MRK'
      MQTT_HOST: 'rabbitmq'
      SIMULATION_SOURCE: 'simulationdb:5432'
      WS_HOST: 'controller'
    configs:
      - source: market_config
        target: /opt/gurobi/gurobi.lic

''')
configs[f'market_config']= f'./market_gurobi.lic'

# Build one TSO
output.append(f'''
  tso:
    container_name: net
    image: {image_repo}agent:latest
    environment:
      AREA_CODE: 'DE111'
      TYPE: 'NET'
      MQTT_HOST: 'rabbitmq'
      SIMULATION_SOURCE: 'simulationdb:5432'
      WS_HOST: 'controller'
''')
# Build Demand Agents
agents = np.load('dem_agents.npy')
for agent in agents[:counter]:
    output.append(f'''
  dem_{agent.lower()}:
    container_name: dem_{agent.lower()}
    image: {image_repo}agent:latest
    environment:
      AREA_CODE: {agent}
      TYPE: 'DEM'
      MQTT_HOST: 'rabbitmq'
      SIMULATION_SOURCE: 'simulationdb:5432'
      WS_HOST: 'controller'
''')
# Build Power Plant Agents
agents = np.load('pwp_agents.npy')
for agent in agents[:counter]:
    output.append(f'''
  pwp_{agent.lower()}:
    container_name: pwp_{agent.lower()}
    image: {image_repo}agent:latest
    environment:
      AREA_CODE: {agent}
      TYPE: 'PWP'
      MQTT_HOST: 'rabbitmq'
      SIMULATION_SOURCE: 'simulationdb:5432'
      WS_HOST: 'controller'
''')
# Build Renewable Energy Agents
agents = np.load('res_agents.npy')
for agent in agents[:counter]:
    output.append(f'''
  res_{agent.lower()}:
    container_name: res_{agent.lower()}
    image: {image_repo}agent:latest
    environment:
      AREA_CODE: {agent}
      TYPE: 'RES'
      MQTT_HOST: 'rabbitmq'
      SIMULATION_SOURCE: 'simulationdb:5432'
      WS_HOST: 'controller'
''')
output.append('configs:')
for config, location in configs.items():
  output.append(f'''
  {config}:
    file: {location}
''')
with open('docker-compose.yml', 'w') as f:
    f.writelines(output)


