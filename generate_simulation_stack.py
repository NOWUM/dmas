import numpy as np

image_repo = 'registry.git.fh-aachen.de/nowum-energy/projects/dmas/'

output = []
output.append('version: "3"\n')
output.append('services:\n')

# Build one Control Agent
output.append(f'''
  controller:
    container_name: ctl
    image: {image_repo}agent:latest
    environment:
      PLZ_CODE: 1
      MQTT_EXCHANGE: 'dMAS'
      AGENT_TYPE: 'CTL'
      CONNECT: 'True'
    ports:
      - 5000:5000
''')
# Build one Market
output.append(f'''
  market:
    container_name: mrk
    image: {image_repo}agent:latest
    environment:
      PLZ_CODE: 1
      MQTT_EXCHANGE: 'dMAS'
      AGENT_TYPE: 'MRK'
      CONNECT: 'True'
    volumes:
    - ./gurobi_wls.lic:/opt/gurobi/gurobi.lic
    deploy:
      mode: replicated
      replicas: 1
      placement:
        constraints: [node.role == manager]
''')
# Build one Weather Agent
output.append(f'''
  weather:
    container_name: wtr
    image: {image_repo}agent:latest
    environment:
      PLZ_CODE: 1
      MQTT_EXCHANGE: 'dMAS'
      AGENT_TYPE: 'WTR'
      CONNECT: 'True'
''')
# Build one TSO
output.append(f'''
  tso:
    container_name: net
    image: {image_repo}agent:latest
    environment:
      PLZ_CODE: 1
      MQTT_EXCHANGE: 'dMAS'
      AGENT_TYPE: 'NET'
      CONNECT: 'True'
''')
# Build Demand Agents
agents = np.load('dem_agents.npy')
for agent in agents[:10]:
    output.append(f'''
  dem{agent}:
    container_name: dem{agent}
    image: {image_repo}agent:latest
    environment:
      PLZ_CODE: {agent}
      MQTT_EXCHANGE: 'dMAS'
      AGENT_TYPE: 'DEM'
      CONNECT: 'True'
      ''')
# Build Power Plant Agents
agents = np.load('pwp_agents.npy')
for agent in agents[:10]:
    output.append(f'''
  pwp{agent}:
    container_name: pwp{agent}
    image: {image_repo}agent:latest
    environment:
      PLZ_CODE: {agent}
      MQTT_EXCHANGE: 'dMAS'
      AGENT_TYPE: 'PWP'
      CONNECT: 'True'
      ''')
# Build Renewable Energy Agents
agents = np.load('res_agents.npy')
for agent in agents[:10]:
    output.append(f'''
  res{agent}:
    container_name: res{agent}
    image: {image_repo}agent:latest
    environment:
      PLZ_CODE: {agent}
      MQTT_EXCHANGE: 'dMAS'
      AGENT_TYPE: 'RES'
      CONNECT: 'True'
      ''')

with open('docker-compose_simulation.yml', 'w') as f:
  f.writelines(output)
