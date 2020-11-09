args=("$@")
echo "Start->"  ${args[0]}
echo "End->" ${args[1]}
echo "Typ->" ${args[2]}

for ((i=$1; i <= $2; i++ ))

        do
                if [ "${args[2]}" = "RES" ]; then
                         screen -dmS RES_$i
                         screen -S RES_$i -X stuff $'python3 dmas/model/agents/res_Agent.py --plz "'$i'" \n'
                fi

                if [ "${args[2]}" = "DEM" ]; then
                         screen -dmS DEM_$i
                         screen -S DEM_$i -X stuff $'python3 dmas/model/agents/dem_Agent.py --plz "'$i'" \n'
                fi
                
                if [ "${args[2]}" = "PWP" ]; then
                         screen -dmS PWP_$i
                         screen -S PWP_$i -X stuff $'python3 dmas/model/agents/pwp_Agent.py --plz "'$i'" \n'
                fi

                if [ "${args[2]}" = "STR" ]; then
                         screen -dmS STR_$i
                         screen -S STR_$i -X stuff $'python3 dmas/model/agents/str_Agent.py --plz "'$i'" \n'
                fi

        sleep 0.25
        done

