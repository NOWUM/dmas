This simulation heavily relies on docker to run multiple agents at once.

This can either be configured through Docker Swarm or Docker Compose

# Docker Install

on a fresh linux machine I am always using this script:
https://gist.github.com/maurerle/276fead798430a0ab3d59b86b0d6b494

which takes care of the docker installation

## Docker Swarm

Docker swarm allows to run the simulation across multiple machines (scale out).
To initialize the swarm, one needs to run `ocker swarm init --default-addr-pool 10.128.0.0/12 --default-addr-pool-mask-length 16` on one machine (the swarm manager, which also runs jobs).
This allows to run stacks with more than 256 containers, as otherwise (without the params) not enough private IP-addresses exist - as each container gets a private ip-address assigned.

Then run `docker swarm join-token worker` and enter the shown command on all your workers with installed docker.

That's it.

You can run `docker node ls` on the manager to see the status of your swarm cluster.

# Docker Cheat Sheet

In the following the main commands used to start, stop and debug the data is shown

## Local Simulation - Docker Compose

### starting the simulation

This also builds the image fresh from the current source

`docker compose down --remove-orphans && docker build -t registry.git.fh-aachen.de/nowum-energy/projects/dmas/agent . && docker compose up -d`

### show logs

`docker compose ps`
`docker compose logs controller`

`docker compose logs dem_de1`

### stopping the simulation

`docker compose down`

### exporting database dump

`docker exec simulationdb pg_dump -Fc --disable-triggers -U dMAS dmas > dmas.sql`

## Distributed Cluster - Docker Swarm

### starting the simulation

`docker stack deploy --with-registry-auth -c compose.yml dmas`


### show logs

`docker service logs dmas_controller`

### stopping the simulation

`docker stack rm dmas`

### See how many services are running:

`docker service ls | wc -l && docker service ls | grep 0/1 | wc -l && docker service ls | grep 1/1 | wc -l`

This shows something like

```
1009    # <-- number of total agents
30      # <-- number of agents which are not successfully started
979     # <-- number of successfully running agents
```

This can be used to monitor the startup process, as seen in [swarm-start.sh](../swarm-start.sh)

### exporting database dump

the simulationdb should always run on the manager - therefore we can use this script:

```
SIMDB_NAME=$(docker ps | grep dmas_simulationdb | tr -s ' ' | cut -d" " -f 11)
docker exec $SIMDB_NAME pg_dump -Fc --disable-triggers -U dMAS dmas > dmas.sql
```

### others

```
# check which containers are running and on which node
docker stack ps stack_name

# show latest system log
journalctl -e

# kill dangling processes when child processes were terminated
sudo killall ifquery
sudo service systemd-udevd restart
```

### Problems with Docker swarm

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

There should be enough IP-Addresses available afterwards