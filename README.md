# Requirements

docker-compose -f docker-compose_simulation.yml down --remove-orphans && docker-compose build && docker-compose -f docker-compose_simulation.yml up -d



docker stack deploy --with-registry-auth --compose-file docker-compose_simulation.yml dmas