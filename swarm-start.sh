docker stack rm dmas

while test "$(docker stack ps dmas | wc -l)" -gt 1
do
  sleep 3;
done

echo "removal finished, sleeping"
sleep 10;

docker stack deploy --with-registry-auth -c docker-compose.yml dmas

while test "$(docker service ls | grep 0/1 | wc -l)" -gt 3
do
  docker service ls | wc -l && docker service ls | grep 0/1 | wc -l && docker service ls | grep 1/1 | wc -l
  sleep 30;
done
echo "all containers are up"

docker service rm dmas_dem_de253
docker service rm dmas_res_de253

docker service rm dmas_dem_de941
docker service rm dmas_res_de941

docker service rm dmas_dem_de91c

docker service rm dmas_dem_deb38
docker service rm dmas_res_deb38

sleep 60;

CONTAINER_COUNT=`echo $(docker stack ps dmas | grep Running | wc -l) - 7 | bc`
while test " $(curl -s http://localhost:5000/agent_count)" -lt $CONTAINER_COUNT
do
  sleep 7;
  echo "$(curl -s http://localhost:5000/agent_count) of $CONTAINER_COUNT ready"
done

sleep 10

curl -X POST http://localhost:5000/start -d "begin=2018-01-01" -d "end=2018-04-01"
curl -X POST http://localhost:5000/wait_limit -d "wait_limit=900"
echo ""
