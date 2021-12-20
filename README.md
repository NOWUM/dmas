# Requirements


## Literature
#### wind-python/windpowerlib 
*Sabine Haas; Uwe Krien; Birgit Schachler; Stickler Bot; kyri-petrou; Velibor Zeli; 
Kumar Shivam; Stephen Bosch*

#### pvlib python: a python package for modeling solar energy systems.
*William F. Holmgren, Clifford W. Hansen, and Mark A. Mikofski* 

## Docker Cheat Sheet
**start simulation - local:** <br>
`docker-compose -f docker-compose_simulation.yml down --remove-orphans && docker-compose build && docker-compose -f docker-compose_simulation.yml up -d` <br>
**start simulation - docker swarm:** <br> 
`docker stack deploy --with-registry-auth --compose-file docker-compose_simulation.yml dmas` <br>
**show logs - local:**<br>
`docker logs container_name` <br>
**show logs - docker swarm** <br>
`docker service logs service_name` <br>
<br>
**others:** <br>
`docker node ls` <br>
`docker stack stack_name ps` <br>
`journalctl -e` <br>


