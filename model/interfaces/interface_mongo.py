import pymongo
import json
import pandas as pd
import numpy as np

class mongoInterface:

    def __init__(self, database, host='149.201.88.150'):

        self.mongo = pymongo.MongoClient('mongodb://' + host + ':27017/')
        self.structDB = self.mongo["systemdata"]
        self.tableStructur = self.structDB["energysystems"]

        self.orderDB = self.mongo[database]
        self.status = self.orderDB['status']

        # self.tableOrderbooks = self.mongodb["orderbooks"]

    def getPowerPlants(self, area):
        try:
            return self.tableStructur.find_one({"_id": area})['powerPlants']
        except:
            return {}

    def getStorages(self, area):
        try:
            return self.tableStructur.find_one({"_id": area})['storages']
        except:
            return {}

    """
                    Nachfrage und Prosumer
    """

    def getPVBatteries(self, area):
        try:
            tmp = self.tableStructur.find_one({'_id': area})['PVBatteries']
            for _, value in tmp.items():
                value['para'] = np.asarray([value['para']['A'], value['para']['B'],
                                            value['para']['C'], value['para']['D']], np.float32)
            return tmp
        except Exception as e:
            print(e)
            return {}

    def getHeatPumps(self, area):
        try:
            tmp = self.tableStructur.find_one({'_id': area})['PVHeatpumps']
            for _, value in tmp.items():
                value['para'] = np.asarray([value['para']['A'], value['para']['B'],
                                            value['para']['C'], value['para']['D']], np.float32)
            return tmp
        except Exception as e:
            print(e)
            return {}

    def getPVs(self, area):
        try:
            tmp = self.tableStructur.find_one({'_id': area})['PVs']
            for _, value in tmp.items():
                value['para'] = np.asarray([value['para']['A'], value['para']['B'],
                                            value['para']['C'], value['para']['D']], np.float32)
            return tmp
        except Exception as e:
            print(e)
            return {}

    def getDemand(self, area):
        try:
            return self.tableStructur.find_one({'_id': area})['demand']
        except Exception as e:
            print(e)
            return {}


    """
                    Erneuerbare Energien
    """

    # Wind Onshore Anlagen im entsprechenden PLZ-Gebiet
    def getWindOn(self, area):
        dict_ = {}
        try:
            tech = json.load(open(r'./data/Tech_Wind.json'))
            wind = self.tableStructur.find_one({"_id": area})['windOnshore']
            for key, system in wind.items():
                generator = dict(maxPower=system['Bruttoleistung'],
                                 height=system['Nabenhöhe'], typ='wind')           # Nennleistung [kW]
                generator.update(tech['5'])                                        # Daten der Einspeisekurve
                dict_.update({key: generator})
            return dict_
        except Exception as e:
            print(e)
            return dict_

    # Freiflächen Photovoltaik im entsprechenden PLZ-Gebiet
    def getPvParks(self, area):
        dict_ = {}
        try:
            return self.tableStructur.find_one({"_id": area})['solarparks']
        except Exception as e:
            print(e)
            return dict_

    # EEG vergütete Photovoltaik Dachanlagen im entsprechenden PLZ-Gebiet
    def getPvEEG(self, area):
        dict_ = {}
        try:
            solarsystems = self.tableStructur.find_one({"_id": area})['solarsystems']
            for key, system in solarsystems.items():
                if system['startUpDate'] < pd.to_datetime('2013-01-01'):
                    dict_.update({key: system})
            return dict_
        except Exception as e:
            print(e)
            return dict_

    # Laufwasserkraftwerke im entsprechenden PLZ-Gebiet
    def getRunRiver(self, area):
        dict_ = {}
        try:
            return self.tableStructur.find_one({"_id": area})['run-river']
        except:
            return dict_

    # Biomassekraftwerke im entsprechenden PLZ-Gebiet
    def getBioMass(self, area):
        dict_ = {}
        try:
            return self.tableStructur.find_one({"_id": area})['biomass']
        except Exception as e:
            print(e)
            return dict_

    """
                    Simulations- und Marktnachrichten
    """


    def login(self, name, reserve=False):
        status = {
                    "_id": name,
                    "connected": True,
                    "reserve": reserve
                  }
        if self.status.find_one({"_id": name}) is None:
            self.status.insert_one(status)
        else:
            query = {"_id": name}
            status = {"$set": {"connected": True, "reserve": reserve}}
            self.status.update_one(query, status)

    def logout(self, name):
        query = {"_id": name}
        status = {"$set": {"connected": False, "reserve": False}}
        self.status.update_one(query, status)

    def setBalancing(self, name, date, orders):
        query = {"_id": name}
        orders = {"$set": {"_id": name, "Balancing": orders}}
        self.orderDB[str(date.date())].update_one(filter=query, update=orders, upsert=True)

    def setDayAhead(self, name, date, orders):
        query = {"_id": name}
        orders = {"$set": {"_id": name, "DayAhead": orders}}
        self.orderDB[str(date.date())].update_one(filter=query, update=orders, upsert=True)

    def setActuals(self, name, date, orders):
        query = {"_id": name}
        orders = {"$set": {"_id": name, "Actual": orders}}
        self.orderDB[str(date.date())].update_one(filter=query, update=orders, upsert=True)