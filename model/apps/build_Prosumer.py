import os
os.chdir(os.path.dirname(os.path.dirname(__file__)))
import requests
import json
import pandas as pd
import numpy as np
import pymongo
import multiprocessing
from joblib import Parallel, delayed
import pickle


def getPVBatSys(id):

    # Url to "Marktstammdatenregister"

    url = 'https://www.marktstammdatenregister.de/MaStR/Einheit/EinheitJson/' \
          'GetErweiterteOeffentlicheEinheitStromerzeugung?sort=&page=1&pageSize=10&' \
          'group=&filter=MaStR-Nr.%20des%20Anlagenbetreibers~eq~'

    ask = url + '%27' + id + '%27'
    resp = requests.get(url=ask)
    data = resp.json()

    # Determine Heat Demand [kW/m²a] and Heat Parameters for SLP-Heat
    random = np.random.uniform(low=0, high=1)
    if random < 0.03:
        para = {"A": 2.794,"B": -37.18,"C": 5.403, "D": 0.1714}
        dem = 25
    elif random < 0.12:
        para = {"A": 2.86,"B": -37.18,"C": 5.47, "D": 0.16}
        dem = 70
    elif random < 0.25:
        para = {"A": 2.91,"B": -37.18,"C": 5.52, "D": 0.15}
        dem = 100
    elif random < 0.87:
        para = {"A": 2.98,"B": -37.19,"C": 5.6,"D": 0.13}
        dem = 150
    else:
        para = {"A": 3.13, "B": -37.19, "C": 5.75, "D": 0.10}
        dem = 250

    # Determine Living Space
    A = int(np.random.normal(loc=105, scale=105 * 0.05))

    # Build Up Energy-System
    dict_ = {}
    try:
        if data['Data'][0]['NutzbareSpeicherkapazitaet'] is None:
            vmax = data['Data'][0]['Nettonennleistung']
        else:
            vmax = data['Data'][0]['NutzbareSpeicherkapazitaet']
        dict_ = {id:
                 {'typ': 'PvBat',
                  'plz': int(data['Data'][1]['Plz']),
                  'para': para,
                  'demandP': int(data['Data'][1]['Nettonennleistung']*np.random.uniform(low=900, high=1500)),
                  'demandQ': int(A*dem),
                  'PV': dict(eta=0.15, maxPower=data['Data'][1]['Nettonennleistung'], direction=180, area=7),
                  'Bat': dict(v0=0, vmax=vmax, eta=0.96, maxPower=data['Data'][0]['Nettonennleistung'])
                  }
             }

    except:
        print('no data found')

    return dict_

def readPVBatSys():

    with open(r'./data/PVBat_Systems.json') as json_file:
        data = json.load(json_file)
    lst = [dict() for i in range(99)]

    for element in data:
        for _, value in element.items():
            if len(str(value['plz'])) >= 5:
                plz = str(value['plz'])[:2]

            else:
                plz = '0' + str(value['plz'])[:1]
            index = int(plz) - 1
        lst[index].update(element)

    return lst

def getPVWPSystems(plz):

    mongo = pymongo.MongoClient('mongodb://' + '149.201.88.150' + ':27017/')
    structDB = mongo["systemdata"]
    tableStructur = structDB["energysystems"]

    try:
        heatpumps = tableStructur.find_one({'_id': int(plz)})['heatpumps']['Number']
    except:
        heatpumps = 0

    systems = tableStructur.find_one({'_id': int(plz)})['solarsystems']

    pvWps = {}
    pv = {}

    for key, value in systems.items():

        id = key

        random = np.random.uniform(low=0, high=1)

        if random < 0.03:
            para = {"A": 2.794, "B": -37.18, "C": 5.403, "D": 0.1714}
            dem = 25
        elif random < 0.12:
            para = {"A": 2.86, "B": -37.18, "C": 5.47, "D": 0.16}
            dem = 70
        elif random < 0.25:
            para = {"A": 2.91, "B": -37.18, "C": 5.52, "D": 0.15}
            dem = 100
        elif random < 0.87:
            para = {"A": 2.98, "B": -37.19, "C": 5.6, "D": 0.13}
            dem = 150
        else:
            para = {"A": 3.13, "B": -37.19, "C": 5.75, "D": 0.10}
            dem = 250

        A = int(np.random.normal(loc=105, scale=105 * 0.05))

        Ref_PVSystem = pickle.load(open(r'./data/Ref_PVSystem.dict', 'rb'))

        if False:#(id not in ids) & (int(heatpumps) > 0):

            classes = Ref_PVSystem['classes']
            prob = Ref_PVSystem['prob']
            index = np.where(prob > np.random.uniform() * 100)[0][0]

            azimut = np.random.uniform(low=classes[index][0] + 180, high=classes[index][1] + 180)
            tilt = np.random.normal(loc=35, scale=5)

            dict_ = {}
            try:
                dict_ = {id:
                             {'typ': 'PvWp',
                              'plz': int(value['plz']),
                              'para': para,
                              'demandP': int(value['maxPower'] * np.random.uniform(low=900, high=1500)),
                              'demandQ': int(A * dem),
                              'PV': dict(eta=0.15, maxPower=value['maxPower'], azimut=azimut, tilt=tilt),
                              'HP': dict(cop=3, q_max=int(A * dem) * 0.0004 + 1.2, t1=20, t2=40),
                              'tank': dict(vmax=60 * 5 * 4.2 / 3600 * (40 - 20), vmin=0, v0=0)
                              }
                         }
                heatpumps -= 1

            except:
                print('no data found')

            pvWps.update(dict_)

        else:

            classes = Ref_PVSystem['classes']
            prob = Ref_PVSystem['prob']
            index = np.where(prob > np.random.uniform() * 100)[0][0]

            azimut = np.random.uniform(low=classes[index][0] + 180, high=classes[index][1] + 180)
            tilt = np.random.normal(loc=35, scale=5)

            dict_ = {}
            try:
                dict_ = {id:
                             {'typ': 'Pv',
                              'plz': int(value['plz']),
                              'para': para,
                              'demandP': int(value['maxPower'] * np.random.uniform(low=900, high=1500)),
                              'demandQ': int(A * dem),
                              'PV': dict(eta=0.15, maxPower=value['maxPower'], azimut=azimut, tilt=tilt)
                              }
                         }
            except Exception as e:
                print(e)
                print('no data found')

            if pd.to_datetime(value['startUpDate']) > pd.to_datetime('2013-01-01'):
                pv.update(dict_)

    return pvWps, pv

def tmp(i):
    print(i)
    mongo = pymongo.MongoClient('mongodb://' + '149.201.88.150' + ':27017/')
    structDB = mongo["systemdata"]
    tableStructur = structDB["energysystems"]
    pvWPs, pv = getPVWPSystems(i)
    query = {"_id": i}
    d = {"$set": {'_id': i, 'PVHeatpumps': pvWPs, 'PVs': pv}}
    tableStructur.update_one(filter=query, update=d, upsert=True)

if __name__ == '__main__':

    KeyDataRegister = False
    writeToMongo = False

    mongo = pymongo.MongoClient('mongodb://' + '149.201.88.150' + ':27017/')
    structDB = mongo["systemdata"]
    tableStructur = structDB["energysystems"]

    Ref_PVSystem = pickle.load(open(r'./data/Ref_PVSystem.dict', 'rb'))

    # if KeyDataRegister:
    #     df = pd.read_excel(r'./data/ids.xlsx')
    #     ids = df.to_numpy().reshape((-1,))
    #     num_cores = min(multiprocessing.cpu_count(), 60)
    #     data = Parallel(n_jobs=num_cores)(delayed(getPVBatSys)(id) for id in ids)
    #
    #     with open(r'./data/PVBat_Systems.json', 'w') as outfile:
    #         json.dump(data, outfile)
    #
    # else:
    #     data = readPVBatSys()
    #     if writeToMongo:
    #         for i in range(99):
    #             plz = i + 1
    #             query = {"_id": plz}
    #             d = {"$set": {'_id': plz, 'PVBatteries': data[i]}}
    #             tableStructur.update_one(filter=query, update=d, upsert=True)
    #
    # num_cores = min(multiprocessing.cpu_count(), 60)
    # Parallel(n_jobs=num_cores)(delayed(tmp)(id) for id in range(1, 100))