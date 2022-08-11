./generate_simulation_stack.py
docker compose down --remove-orphans && docker compose build && docker compose up -d

sleep 10;
CONTAINER_COUNT=`echo $(docker compose ps | wc -l) - 5 | bc`

while test " $(curl -s http://localhost:5000/agent_count)" -lt $CONTAINER_COUNT
do
  echo "$(docker compose logs --tail=1 | grep 'waiting for instructions' | wc -l) of $CONTAINER_COUNT ready"
  sleep 7;
done
curl -X POST http://localhost:5000/start -d "begin=2018-01-01" -d "end=2018-02-01"

xdg-open http://localhost:3001/d/VdwbNAX72/dmas?orgId=1&refresh=5s
