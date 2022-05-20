docker stack rm dmas

while test "$(docker stack ps dmas | wc -l)" -gt 1
do
  sleep 3;
done

docker stack deploy --with-registry-auth -c docker-compose.yml dmas

docker service ls | wc -l && docker service ls | grep 0/1 | wc -l && docker service ls | grep 1/1 | wc -l

sleep 120

docker service ls | wc -l && docker service ls | grep 0/1 | wc -l && docker service ls | grep 1/1 | wc -l

# remove services with weird windpower
#docker service rm dmas_res_def07 dmas_res_de40h dmas_res_de734 dmas_res_de733 dmas_res_de922 dmas_res_de254 dmas_res_dea37 dmas_res_dea38 dmas_res_de25c dmas_res_def0b





