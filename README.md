# Requirements


## Literature

### wind-python/windpowerlib 

*Sabine Haas; Uwe Krien; Birgit Schachler; Stickler Bot; kyri-petrou; Velibor Zeli; 
Kumar Shivam; Stephen Bosch*

### pvlib python: a python package for modeling solar energy systems.

*William F. Holmgren, Clifford W. Hansen, and Mark A. Mikofski* 

## Docker Cheat Sheet

### start simulation - local:

`docker-compose -f docker-compose_simulation.yml down --remove-orphans && docker-compose build && docker-compose -f docker-compose_simulation.yml up -d` <br>

### start simulation - docker swarm

`docker stack deploy --with-registry-auth --compose-file docker-compose_simulation.yml dmas`

### show logs - local

`docker logs container_name`

### show logs - docker swarm

`docker service logs service_name`

### others

```
docker node ls
docker stack stack_name ps
journalctl -e
```


