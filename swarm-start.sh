docker stack rm dmas

while test "$(docker stack ps dmas | wc -l)" -gt 1
do
  sleep 3;
done

sleep 10;

docker stack deploy --with-registry-auth -c docker-compose.yml dmas

while test "$(docker service ls | grep 0/1 | wc -l)" -gt 3
do
  docker service ls | wc -l && docker service ls | grep 0/1 | wc -l && docker service ls | grep 1/1 | wc -l
  sleep 30;
done

sleep 400

curl -X POST http://localhost:5000/start -d "begin=2018-01-01" -d "end=2018-02-01"



