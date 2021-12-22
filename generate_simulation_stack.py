import numpy as np

image_repo = 'registry.git.fh-aachen.de/nowum-energy/projects/dmas/'

output = []
output.append('version: "3"\n')
output.append('services:\n')

output.append(f'''
  simulationdb:
    container_name: simulationdb
    image: timescale/timescaledb:latest-pg12
    command: postgres -c 'max_connections=500' -B 4096MB
    restart: always
    environment:
      - POSTGRES_USER=dMAS
      - POSTGRES_PASSWORD=dMAS
      - POSTGRES_DB=dmas
    ports:
      - 5432:5432
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
''')
# Build one Control Agent
output.append(f'''
  controller:
    container_name: ctl
    image: {image_repo}agent:latest
    environment:
      AREA_CODE: 'DE111'
      TYPE: 'CTL'
    ports:
      - 5000:5000
''')
# Build one Market
output.append(f'''
  market:
    container_name: mrk
    image: {image_repo}agent:latest
    environment:
      AREA_CODE: 'DE111'
      TYPE: 'MRK'
    volumes:
    - ./gurobi.lic:/opt/gurobi/gurobi.lic:ro
    deploy:
      mode: replicated
      replicas: 1
      placement:
        constraints: [node.role == manager]
''')
# Build one TSO
output.append(f'''
  tso:
    container_name: net
    image: {image_repo}agent:latest
    environment:
      AREA_CODE: 'DE111'
      TYPE: 'NET'
''')
# Build Demand Agents
agents = np.load('dem_agents.npy')
for agent in agents[:5]:
    output.append(f'''
  dem_{agent.lower()}:
    container_name: dem_{agent.lower()}
    image: {image_repo}agent:latest
    environment:
      AREA_CODE: {agent}
      TYPE: 'DEM'
      ''')
# Build Power Plant Agents
agents = np.load('pwp_agents.npy')
for agent in agents[:5]:
    output.append(f'''
  pwp_{agent.lower()}:
    container_name: pwp_{agent.lower()}
    image: {image_repo}agent:latest
    environment:
      AREA_CODE: {agent}
      TYPE: 'PWP'
      ''')
# Build Renewable Energy Agents
agents = np.load('res_agents.npy')
for agent in agents[:5]:
    output.append(f'''
  res_{agent.lower()}:
    container_name: res_{agent.lower()}
    image: {image_repo}agent:latest
    environment:
      AREA_CODE: {agent}
      TYPE: 'RES'
      ''')

with open('docker-compose_simulation.yml', 'w') as f:
    f.writelines(output)
