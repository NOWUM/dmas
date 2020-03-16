#!/bin/bash
args=("$@")
echo "Start->"  ${args[0]}
echo "End->" ${args[1]}
echo "Typ->" ${args[2]}


for ((i=$1; i <= $2; i++ ))
        do
                if [ "${args[2]}" = "RES" ]; then
                         screen -S RES_$i
                         screen -S RES_$i -X stuff $'python3 dmas/model/agents/res_Agent.py --plz "'$i'" \n'
                 fi

                 if [ "${args[2]}" = "DEM" ]; then
                         screen -S DEM_$i
                         screen -S DEM_$i -X stuff $'python3 dmas/model/agents/dem_Agent.py --plz "'$i'" \n'
                 fi

                 if [ "${args[2]}" = "PWP" ]; then
                         screen -S PWP_$i
                         screen -S PWP_$i -X stuff $'python3 dmas/model/agents/pwp_Agent.py --plz "'$i'" \n'
                 fi

        done
