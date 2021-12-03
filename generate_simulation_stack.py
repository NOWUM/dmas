image_repo = 'registry.git.fh-aachen.de/nowum-energy/projects/dmas/'

output = []
output.append('version: "3"\n')
output.append('services:\n')
# Build TimescaleDB
output.append(f'''
  simulationdb:
    container_name: simulationdb
    image: timescale/timescaledb:latest-pg12
    restart: always
    environment:
      - POSTGRES_USER=dMAS
      - POSTGRES_PASSWORD=dMAS
      - POSTGRES_DB=dMAS
    ports:
      - 5432:5432
''')
output.append('''
  pgadmin:
    image: dpage/pgadmin4:latest
    container_name: pgadmin4
    environment:
        PGADMIN_DEFAULT_EMAIL: nowum-energy@fh-aachen.de
        PGADMIN_DEFAULT_PASSWORD: nowum
        PGADMIN_LISTEN_PORT: 80
    ports:
        - 9090:80
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
for plz in range(50, 56):
    output.append(f'''
  dem{plz}:
    container_name: dem{plz}
    image: {image_repo}agent:latest
    environment:
      PLZ_CODE: {plz}
      MQTT_EXCHANGE: 'dMAS'
      AGENT_TYPE: 'DEM'
      CONNECT: 'True'
      ''')
# Build Power Plant Agents
for plz in range(50, 56):
    output.append(f'''
  pwp{plz}:
    container_name: pwp{plz}
    image: {image_repo}agent:latest
    environment:
      PLZ_CODE: {plz}
      MQTT_EXCHANGE: 'dMAS'
      AGENT_TYPE: 'PWP'
      CONNECT: 'True'
      ''')
# Build Renewable Energy Agents
for plz in range(50, 56):
    output.append(f'''
  res{plz}:
    container_name: res{plz}
    image: {image_repo}agent:latest
    environment:
      PLZ_CODE: {plz}
      MQTT_EXCHANGE: 'dMAS'
      AGENT_TYPE: 'RES'
      CONNECT: 'True'
      ''')

with open('docker-compose_simulation.yml', 'w') as f:
  f.writelines(output)
