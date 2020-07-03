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

def readPVSys():

    mongo = pymongo.MongoClient('mongodb://' + '149.201.88.150' + ':27017/')
    structDB = mongo["systemdata"]
    tableStructur = structDB["energysystems"]
    lst = []
    for i in range(1, 5):

        systems = tableStructur.find_one({'_id': int(i)})['solarsystems']
        pvs = {}
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

            index = np.where(prob > np.random.uniform() * 100)[0]
            if len(index) == 0:
                index = 17
            else:
                index = index[0]
            azimut = np.random.uniform(low=classes[index][0] + 180, high=classes[index][1] + 180)
            tilt = np.random.normal(loc=35, scale=5)

            try:

                if pd.to_datetime(value['startUpDate']) > pd.to_datetime('2013-01-01'):
                    EEG = True
                else:
                    EEG = False

                dict_ = {id:
                             {'typ': 'Pv',
                              'plz': int(value['plz']),
                              'para': [x for _, x in para.items()],
                              'demandP': int(value['maxPower'] * np.random.uniform(low=900, high=1500)),
                              'demandQ': int(A * dem),
                              'PV': dict(eta=0.15, maxPower=value['maxPower'], azimut=int(azimut), tilt=int(tilt), EEG=EEG, Park=False)
                              }
                         }
                pvs.update(dict_)
            except Exception as e:
                print(e)
                print('no data found')
        lst.append(pvs)

    return lst

def readPVBatSys():

    with open(r'./data/PVBat_Systems.json') as json_file:
        data = json.load(json_file)
    lst = [dict() for _ in range(99)]

    for element in data:
        for _, value in element.items():
            if len(str(value['plz'])) >= 5:
                plz = str(value['plz'])[:2]
            else:
                plz = '0' + str(value['plz'])[:1]
            index = int(plz) - 1
        lst[index].update(element)

    return lst

if __name__ == '__main__':

    mongo = pymongo.MongoClient('mongodb://' + '149.201.88.150' + ':27017/')
    structDBOld = mongo["systemdata"]
    tableStructurOld = structDBOld["energysystems"]

    mongo = pymongo.MongoClient('mongodb://' + '149.201.88.150' + ':27017/')
    structDB = mongo["dMAS_Systemdata"]

    Ref_PVSystem = pickle.load(open(r'./data/Ref_PVSystem.dict', 'rb'))
    classes = Ref_PVSystem['classes']
    prob = Ref_PVSystem['prob'][:-1]

    dataPVBat = readPVBatSys()
    # for plz in range(1, 5):
    #     if plz not in [5, 11, 62]:
    #         tableStructur = structDB["PLZ_%s" %plz]
    #         element = dataPVBat[plz-1]
    #
    #         for key, value in element.items():
    #             index = np.where(prob > np.random.uniform() * 100)[0]
    #             if len(index) == 0:
    #                 index = 17
    #             else:
    #                 index = index[0]
    #             azimut = np.random.uniform(low=classes[index][0] + 180, high=classes[index][1] + 180)
    #             tilt = np.random.normal(loc=35, scale=5)
    #             value['para'] = [x for _, x in value['para'].items()]
    #             value['PV'] = (dict(eta=0.15, maxPower=value['PV']['maxPower'], azimut=int(azimut), tilt=int(tilt), EEG=False, Park=False))
    #
    #         if len(element) > 0:
    #             query = {"_id": 'PVBatSystems'}
    #             d = {"$set": element}
    #             tableStructur.update_one(filter=query, update=d, upsert=True)

    dataPv = readPVSys()
    for plz in range(1, 5):
        if plz not in [5, 11, 62]:
            tableStructur = structDB["PLZ_%s" %plz]
            element = dataPv[plz-1]
            if len(element) > 0:
                query = {"_id": 'PVSystems'}
                d = {"$set": element}
                tableStructur.update_one(filter=query, update=d, upsert=True)

            # element = tableStructurOld.find_one({"_id": plz})['biomass']
            # if len(element) > 0:
            #     query = {"_id": 'BiomassSystems'}
            #     d = {"$set": element}
            #     tableStructur.update_one(filter=query, update=d, upsert=True)
            #
            # element = tableStructurOld.find_one({"_id": plz})['run-river']
            # if len(element) > 0:
            #     query = {"_id": 'RunRiverSystems'}
            #     d = {"$set": element}
            #     tableStructur.update_one(filter=query, update=d, upsert=True)
            #
            # element = tableStructurOld.find_one({"_id": plz})['storages']
            # if len(element) > 0:
            #     query = {"_id": 'StorageSystems'}
            #     d = {"$set": element}
            #     tableStructur.update_one(filter=query, update=d, upsert=True)
            #
            # element = tableStructurOld.find_one({"_id": plz})['powerPlants']
            # if len(element) > 0:
            #     query = {"_id": 'PowerPlantSystems'}
            #     d = {"$set": element}
            #     tableStructur.update_one(filter=query, update=d, upsert=True)
            #
            # element = tableStructurOld.find_one({"_id": plz})['demand']
            # query = {"_id": 'ConsumerSystems'}
            # d = {"$set": element}
            # tableStructur.update_one(filter=query, update=d, upsert=True)
            #
            # element = tableStructurOld.find_one({"_id": plz})['position']
            # query = {"_id": 'Position'}
            # d = {"$set": element}
            # tableStructur.update_one(filter=query, update=d, upsert=True)

    # writeToMongo = True
    #
    # mongo = pymongo.MongoClient('mongodb://' + '149.201.88.150' + ':27017/')
    # structDB = mongo["systemdata"]
    # tableStructur = structDB["energysystems"]
    #
    #
    #
    # if writeToMongo:
    #     for i in range(99):
    #         plz = i + 1
    #         query = {"_id": plz}
    #         d = {"$set": {'_id': plz, 'PVBatteries': dataPVBat[i]}}
    #         tableStructur.update_one(filter=query, update=d, upsert=True)
    #
    # mongo = pymongo.MongoClient('mongodb://' + '149.201.88.150' + ':27017/')
    # structDB = mongo["systemdata"]
    # tableStructur = structDB["energysystems"]
    #
    # dataPVnotEEG = []
    # dataPVEEG = []
    #
    # for i in range(1, 100):
    #
    #     systems = tableStructur.find_one({'_id': int(i)})['solarsystems']
    #
    #     notEEG = []
    #     EEG = []
    #

    #
    # if writeToMongo:
    #     for i in range(83, 99):
    #         plz = i + 1
    #         query = {"_id": plz}
    #         #d = {"$set": {'_id': plz, 'PVHeatpumps': {}, 'PVs' : dataPVnotEEG[i]}}
    #         #tableStructur.update_one(filter=query, update=d, upsert=True)
    #         for data in dataPVEEG[i]:
    #             d = {"$set": {'_id': plz, 'PVHeatpumps': {}, 'EEGPVs': data}}
    #             tableStructur.update_one(filter=query, update=d, upsert=False)


    # if KeyDataRegister:
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