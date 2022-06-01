docker compose down && docker compose build && docker compose up -d

CONTAINER_COUNT=`echo $(docker compose ps | wc -l) - 7 | bc`
while test "$(docker compose logs --tail=1 | grep 'waiting for instructions' | wc -l)" -lt $CONTAINER_COUNT
do
  sleep 3;
done
curl -X POST http://localhost:5000/start -d "begin=2018-01-01" -d "end=2018-02-01"

# http://localhost:3001/d/VdwbNAX72/dmas?orgId=1&refresh=5s
