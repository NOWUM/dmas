import json
import numpy as np
import uuid

class Houses:

    def __init__(self):
        with open(r'./data/Tech_Demand.json') as json_file:
            self.data = json.load(json_file)

    def build(self,comp):

        if np.random.uniform() > 0.533:
            typ = 'MFH'
            As = [int(np.random.normal(loc=70, scale=70*0.05)) for i in range(np.random.randint(low=3,high=10))]
        else:
            typ = 'EFH'
            As = [int(np.random.normal(loc=105, scale=105*0.05)) for i in range(np.random.choice(a=[1,2],p=[0.8,0.2]))]

        # ----- Heat Demand -----
        heatData = self.data['apartments']['heat']
        prob = [(key,value['prob']/100)[1] for key,value in heatData.items()]
        keys = [(key,value)[0] for key,value in heatData.items()]
        key = np.random.choice(a=keys,p=prob)
        para = heatData[key]['para']
        para = np.asarray([para['A'],para['B'],para['C'],para['D']], np.float32)
        demandQ = int(np.random.uniform(low=float(key)-30,high=float(key)+30) * np.sum(As))

        # ----- Persons and Power -----
        personData = self.data['apartments']['persons']
        powerData = self.data['apartments']['power'][typ]
        keys = [(key, value)[0] for key, value in personData.items()]

        demandP = 0
        for A in As:
            index = int(np.argmin([np.abs(A-float(i)) for i in keys]))
            key = keys[index]
            prob = [(key,value/100)[1] for key,value in personData[key].items()]
            persons = str(np.random.choice(a=[1,2,np.random.randint(low=3,high=5)], p=prob))
            demandP += np.random.randint(low=powerData[persons][0],high=powerData[persons][1])

        name = str(uuid.uuid4())
        if comp == 'Pv':
            dict_ = {name:
                         {'typ'    : 'Pv',
                          'para'   : para,
                          'demandP': demandP,
                          'demandQ': demandQ,
                          'PV'     : dict(eta=0.15,peakpower=demandP/1000,direction=180)
                         }
                     }
        elif comp == 'PvBat':
            dict_ = {name:
                         {'typ'    : 'PvBat',
                          'para'   : para,
                          'demandP': demandP,
                          'demandQ': demandQ,
                          'PV'     : dict(eta=0.15, peakpower=demandP / 1000, direction=180),
                          'Bat'    : dict(v0=0, vmax=3, eta=0.96)
                          }
                     }
        elif comp =='PvWp':
            dict_ = {name:
                         {'typ'    : 'PvWp',
                          'para'   : para,
                          'demandP': demandP,
                          'demandQ': demandQ,
                          'PV'     : dict(eta=0.15, peakpower=demandP / 1000, direction=180),
                          'WP'     : dict(cop=3, q_max=demandQ*0.0004+1.2, t1=20, t2=40),
                          'tank'   : dict(vmax=60 * 5 * 4.2 / 3600 * (40 - 20), vmin=0, v0=0)
                          }
                     }
        return name, dict_ , demandP

if __name__ == "__main__":

    builder = Houses()

    name, house = builder.build(comp='pv')




