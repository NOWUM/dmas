import pymongo
import json

class mongoInterface:

    def __init__(self, host='149.201.88.150'):

        self.mongo = pymongo.MongoClient('mongodb://' + host + ':27017/')
        self.mongodb = self.mongo["MAS_2019"]
        self.tableStructur = self.mongodb["system_data"]
        self.tableOrderbooks = self.mongodb["orderbooks"]

    def getWindOn(self, area):
        try:
            tech = json.load(open('./data/Tech_Wind.json'))
            return self.tableStructur.find({"_id": area})[0]['wind_onshore'], tech
        except:
            return [], []

    def getPvParks(self, area):
        try:
            tech = []
            return self.tableStructur.find({"_id": area})[0]['solar_park'], tech
        except:
            return [], []

    def getPowerPlants(self, area):
        try:
            tech = json.load(open('./data/Tech_Pwp.json'))
            return self.tableStructur.find({"_id": area})[0]['power_plants'], tech
        except:
            return dict(power=[]), []

    def getStorages(self, area):
        try:
            tech = []
            return self.tableStructur.find({"_id": area})[0]['storage'][0], tech
        except:
            return dict(power=[]), []

    def getHouseholds(self, area):
        try:
            tech = self.tableStructur.find({"_id": area})[0]['building']
            data = self.tableStructur.find({"_id": area})[0]['demand']
            return data, tech
        except:
            return [], []

    def login(self, name, reserve=False):
        status = {
                    "_id": name,
                    "connected": True,
                    "reserve": reserve
                  }
        self.tableOrderbooks.insert_one(status)

    def logout(self, name):
        query = {"_id": name}
        status = {"$set": {"connected": False, "reserve": False}}
        self.tableOrderbooks.update_one(query, status)