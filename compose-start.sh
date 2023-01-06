./generate_simulation_stack.py 1 3
docker compose down --remove-orphans && docker compose build && docker compose up -d

sleep 10;
CONTAINER_COUNT=`echo $(docker compose ps | wc -l) - 5 | bc`

while test " $(curl -s http://localhost:5000/agent_count)" -lt $CONTAINER_COUNT
do
  echo "$(docker compose logs --tail=1 | grep 'waiting for instructions' | wc -l) of $CONTAINER_COUNT ready"
  sleep 7;
done
BEGIN=2020-01-01
END=2020-03-01

curl -X POST http://localhost:5000/start -d "begin=$BEGIN" -d "end=$END"

BEGIN_UNIX=$(date --date=$BEGIN '+%s')
END_UNIX=$(date --date=$END '+%s')
xdg-open "http://localhost:3001/d/VdwbNAX72/dmas?orgId=1&refresh=5s&from=${BEGIN_UNIX}000&to=${END_UNIX}000"
