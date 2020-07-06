import pymongo
import json
import pandas as pd
import numpy as np

class mongoInterface:

    def __init__(self, database, area=1, host='149.201.88.150'):

        self.mongo = pymongo.MongoClient('mongodb://' + host + ':27017/')
        self.structDB = self.mongo["testDB"]
        self.tableStructur = self.structDB['PLZ_%s' %area]

        self.orderDB = self.mongo[database]
        self.status = self.orderDB['status']


    def getPosition(self):
        try:
            position = self.tableStructur.find_one({'_id': 'Position'})
            return position
        except:
            return {}


    def getPowerPlants(self):
        try:
            powerplants = self.tableStructur.find_one({"_id": 'PowerPlantSystems'})
            systems = {}
            for key, value in powerplants.items():
                if key != '_id':
                    systems.update({key:value})
            return systems
        except:
            return {}

    def getStorages(self):
        try:
            storages = self.tableStructur.find_one({"_id": 'StorageSystems'})
            systems = {}
            for key, value in storages.items():
                if key != '_id':
                    systems.update({key:value})
            return systems
        except:
            return {}

    """
                    Nachfrage und Prosumer
    """

    def getPVBatteries(self):
        try:
            pvbat = self.tableStructur.find_one({'_id': 'PVBatSystems'})
            position = self.tableStructur.find_one({'_id': 'Position'})
            systems = {}
            for key, value in pvbat.items():
                if key != '_id':
                    value.update({'position': [position['lat'], position['lon']]})
                    systems.update({key:value})
            return systems
        except:
            return {}

    def getHeatPumps(self, area):
        try:
            return {}
        except:
            return {}

    def getPVs(self):
        try:
            pv = self.tableStructur.find_one({'_id': 'PVSystems'})
            position = self.tableStructur.find_one({'_id': 'Position'})
            systems = {}
            for key, value in pv.items():
                if key != '_id':
                    value.update({'position': [position['lat'], position['lon']]})
                    systems.update({key:value})
            return systems
        except:
            return {}

    def getDemand(self):
        try:
            consumer = self.tableStructur.find_one({'_id': 'ConsumerSystems'})
            systems = {}
            for key, value in consumer.items():
                if key != '_id':
                    systems.update({key:value})
            return systems
        except:
            return {}

    """
                    Erneuerbare Energien
    """

    # Wind  Anlagen im entsprechenden PLZ-Gebiet
    def getWind(self):
        try:
            wind = self.tableStructur.find_one({'_id': 'WindSystems'})
            systems = {}
            for key, value in wind.items():
                if key != '_id':
                    value.update({'typ': 'wind'})
                    systems.update({key: value})
            return systems
        except:
            return {}

    # Freiflächen Photovoltaik im entsprechenden PLZ-Gebiet
    def getPvParks(self):
        try:
            pv = self.tableStructur.find_one({'_id': 'PVParkSystems'})
            position = self.tableStructur.find_one({'_id': 'Position'})
            systems = {}
            for key, value in pv.items():
                if key != '_id':
                    value.update({'position': [position['lat'], position['lon']]})
                    systems.update({key:value})
            return systems
        except:
            return {}


    # Laufwasserkraftwerke im entsprechenden PLZ-Gebiet
    def getRunRiver(self):
        try:
            water = self.tableStructur.find_one({'_id': 'RunRiverSystems'})
            systems = {}
            for key, value in water.items():
                if key != '_id':
                    if value['maxPower'] > 0:
                        systems.update({key:value})
            return systems
        except:
            return {}

    # Biomassekraftwerke im entsprechenden PLZ-Gebiet
    def getBioMass(self):
        try:
            water = self.tableStructur.find_one({'_id': 'BiomassSystems'})
            systems = {}
            for key, value in water.items():
                if key != '_id':
                    if value['maxPower'] > 0:
                        systems.update({key:value})
            return systems
        except:
            return {}

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