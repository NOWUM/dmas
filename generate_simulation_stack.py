#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# generates a docker-compose.yml to crawl the data from dwd

image_repo = 'registry.git.fh-aachen.de/nowum-energy/dmas/'
simulation_database = 'dMAS'
weather_database = 'weather'
weather_host = '10.13.10.41'


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
      - POSTGRES_DB={simulation_database}
    volumes:
      - ./data/open-data:/var/lib/postgresql/data
    ports:
      - 5432:5432
''')
# Build PGAdmin
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
    volumes:
        - ./data/pgadmin:/var/lib/pgadmin
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
# Build Grafana
output.append('''
  grafana:
    container_name: grafana
    image: grafana/grafana
    restart: always
    ports:
      - 3000:3000
    volumes:
     - ./data/grafana:/var/lib/grafana
''')
# Build one Market
output.append(f'''
  market:
    container_name: mrk
    image: {image_repo}mrk_agent:latest
    environment:
      SIMULATION_START_DATE: '2018-01-01'
      PLZ_CODE: 1
      MQTT_EXCHANGE: 'dMAS'
      SIMULATIONS_DATABASE: {simulation_database}
''')
# Build one Weather Agent
output.append(f'''
  weather:
    container_name: wtr
    image: {image_repo}wtr_agent:latest
    environment:
      SIMULATION_START_DATE: '2018-01-01'
      PLZ_CODE: 1
      MQTT_EXCHANGE: 'dMAS'
      SIMULATIONS_DATABASE: {simulation_database}
      WEATHER_DATABASE: {weather_database}
      WEATHER_HOST: {weather_host}
''')
# Build one TSO
output.append(f'''
  tso:
    container_name: net
    image: {image_repo}net_agent:latest
    environment:
      SIMULATION_START_DATE: '2018-01-01'
      PLZ_CODE: 1
      MQTT_EXCHANGE: 'dMAS'
      SIMULATIONS_DATABASE: {simulation_database}
''')
# Build Demand Agents
for plz in range(50, 56):
    output.append(f'''
  dem{plz}:
    container_name: dem{plz}
    image: {image_repo}dem_agent:latest
    environment:
      SIMULATION_START_DATE: '2018-01-01'
      PLZ_CODE: {plz}
      MQTT_EXCHANGE: 'dMAS'
      SIMULATIONS_DATABASE: {simulation_database}
      ''')
# Build Power Plant Agents
for plz in range(50, 56):
    output.append(f'''
  pwp{plz}:
    container_name: pwp{plz}
    image: {image_repo}pwp_agent:latest
    environment:
      SIMULATION_START_DATE: '2018-01-01'
      PLZ_CODE: {plz}
      MQTT_EXCHANGE: 'dMAS'
      SIMULATIONS_DATABASE: {simulation_database}
      ''')
# Build Renewable Energy Agents
for plz in range(50, 56):
    output.append(f'''
  res{plz}:
    container_name: res{plz}
    image: {image_repo}res_agent:latest
    environment:
      SIMULATION_START_DATE: '2018-01-01'
      PLZ_CODE: {plz}
      MQTT_EXCHANGE: 'dMAS'
      SIMULATIONS_DATABASE: {simulation_database}
      ''')

with open('docker-compose_simulation.yml', 'w') as f:
  f.writelines(output)
