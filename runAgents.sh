args=("$@")
echo "Start->"  ${args[0]}
echo "End->" ${args[1]}
echo "Typ->" ${args[2]}
echo "Mongo->" ${args[3]:-149.201.88.150}
echo "Influx->" ${args[4]:-149.201.88.150}
echo "Market->" ${args[5]:-149.201.88.150}
echo "dbName->" ${args[6]:-MAS_XXXX}


for ((i=$1; i <= $2; i++ ))

        do
                if [ "${args[2]}" = "RES" ]; then
                         screen -dmS RES_$i
                         screen -S RES_$i -X stuff $'python3 dmas/model/agents/res_Agent.py --plz "'$i'" --mongo "'${args[3]:-149.201.88.150}'" --influx "'${args[4]:-149.201.88.150}'" --market "'${args[5]:-149.201.88.150}'" --dbName "'${args[6]:-MAS_XXXX}'" \n'
                fi

                if [ "${args[2]}" = "DEM" ]; then
                         screen -dmS DEM_$i
                         screen -S DEM_$i -X stuff $'python3 dmas/model/agents/dem_Agent.py --plz "'$i'" --mongo "'${args[3]:-149.201.88.150}'" --influx "'${args[4]:-149.201.88.150}'" --market "'${args[5]:-149.201.88.150}'" --dbName "'${args[6]:-MAS_XXXX}'" \n'

                fi
                
                if [ "${args[2]}" = "PWP" ]; then
                         screen -dmS PWP_$i
                         screen -S PWP_$i -X stuff $'python3 dmas/model/agents/pwp_Agent.py --plz "'$i'" --mongo "'${args[3]:-149.201.88.150}'" --influx "'${args[4]:-149.201.88.150}'" --market "'${args[5]:-149.201.88.150}'" --dbName "'${args[6]:-MAS_XXXX}'" \n'
                fi
        done
