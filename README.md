# distributed Multi Agent Simulation

This repository contains a distributed multi agent simulation.

# Requirements

- [docker](./docs/docker.md)
- alternatively local python
- server with data from [Open-Energy-Data-Server](https://github.com/NOWUM/open-energy-data-server/)

# Configuration

The best way to run the simulation is to configure `compose-start.sh` and run it.
This creates a docker-compose.yml which is then started.

# Agent Classes

DEM - Demand Agent

RES - Renewable Agents for Wind and Solar

PWP - Powerplant Agent

MRK - Market Agent

# Literature from Libraries

### wind-python/windpowerlib 

*Sabine Haas; Uwe Krien; Birgit Schachler; Stickler Bot; kyri-petrou; Velibor Zeli; 
Kumar Shivam; Stephen Bosch*

### pvlib python: a python package for modeling solar energy systems.

*William F. Holmgren, Clifford W. Hansen, and Mark A. Mikofski* 
