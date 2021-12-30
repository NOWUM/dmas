# Requirements


## Literature

### wind-python/windpowerlib 

*Sabine Haas; Uwe Krien; Birgit Schachler; Stickler Bot; kyri-petrou; Velibor Zeli; 
Kumar Shivam; Stephen Bosch*

### pvlib python: a python package for modeling solar energy systems.

*William F. Holmgren, Clifford W. Hansen, and Mark A. Mikofski* 

## Docker Cheat Sheet

### start simulation - local:

`docker-compose down --remove-orphans && docker build -t registry.git.fh-aachen.de/nowum-energy/projects/dmas/agent . && docker-compose up -d`

### start simulation - docker swarm

`docker stack deploy --with-registry-auth -c docker-compose.yml dmas`

### show logs - local

`docker logs container_name`

### show logs - docker swarm

`docker service logs service_name`

### others

```
# See how many services are running:
`docker service ls | wc -l && docker service ls | grep 0/1 | wc -l && docker service ls | grep 1/1 | wc -l`

# check status of swarm nodes
docker node ls

# check which containers are running and where
docker stack ps stack_name

# show latest system log
journalctl -e

# kill dangling processes when child processes were terminated
sudo killall ifquery
sudo service systemd-udevd restart
```

# Problems with Docker swarm

Docker swarm kills the process when starting many containers:

https://github.com/systemd/systemd/issues/3374#issuecomment-812683254

`sudo nano /etc/systemd/network/98-default.link`
and write the following
```
[Match]
Driver=bridge bonding

[Link]
MACAddressPolicy=none
```

Other approaches contain changes with the arp cache (or generally sys.net.ipv4 settings)
https://www.cloudbees.com/blog/running-1000-containers-in-docker-swarm

But we don't get error messages related for that

Using a pre-existing docker network does not help either
https://docs.docker.com/compose/networking/#use-a-pre-existing-network

There should be enough IP-Addresses available