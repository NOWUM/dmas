#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# generates a docker-compose.yml to crawl the data from dwd

output = []
output.append('version: "3"\n')
output.append('services:\n')
# Build Rabbitmq
output.append('''
  rabbitmq:
    container_name: rabbitmq
    image: rabbitmq
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
''')
# Build one Market
output.append('''
  market:
    container_name: mrk
    image: mrk_agent_latest
    build:
      context: .
      dockerfile: ./dockerfiles/Dockerfile_mrk
''')
# Build one Weather Agent
output.append('''
  weather:
    container_name: wtr
    image: wtr_agent_latest
    build:
      context: .
      dockerfile: ./dockerfiles/Dockerfile_wtr
''')
# Build one TSO
output.append('''
  tso:
    container_name: net
    image: net_agent_latest
    build:
      context: .
      dockerfile: ./dockerfiles/Dockerfile_net
''')
# Build Demand Agents
for plz in range(50, 56):
    output.append(f'''
  dem{plz}:
    container_name: dem{plz}
    image: dem_agent:latest
    build:
      context: .
      dockerfile: ./dockerfiles/Dockerfile_dem
      ''')
# Build Power Plant Agents
for plz in range(50, 56):
    output.append(f'''
  pwp{plz}:
    container_name: pwp{plz}
    image: pwp_agent:latest
    build:
      context: .
      dockerfile: ./dockerfiles/Dockerfile_pwp
      ''')
# Build Renewable Energy Agents
for plz in range(50, 56):
    output.append(f'''
  res{plz}:
    container_name: res{plz}
    image: res_agent:latest
    build:
      context: .
      dockerfile: ./dockerfiles/Dockerfile_res
      ''')

with open('docker-compose_simulation.yml', 'w') as f:
  f.writelines(output)
