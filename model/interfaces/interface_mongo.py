import pymongo
import json

class mongoInterface:

    def __init__(self, host='149.201.88.150'):

        self.mongo = pymongo.MongoClient('mongodb://' + host + ':27017/')
        self.mongodb = self.mongo["MAS_2019"]
        self.mongoTable = self.mongodb["system_data"]

    def getWindOn(self, area):
        try:
            tech = json.load(open('./data/Tech_Wind.json'))
            return self.mongoTable.find({"_id": area})[0]['wind_onshore'], tech
        except:
            return [], []

    def getPvParks(self, area):
        try:
            tech = []
            return self.mongoTable.find({"_id": area})[0]['solar_park'], tech
        except:
            return [], []

    def getPowerPlants(self, area):
        try:
            tech = json.load(open('./data/Tech_Pwp.json'))
            return self.mongoTable.find({"_id": area})[0]['power_plants'],tech
        except:
            return [], []

    def getStorages(self, area):
        try:
            tech = []
            return self.mongoTable.find({"_id": area})[0]['storage'][0], tech
        except:
            return [], []

    def getHouseholds(self, area):
        try:
            tech = self.mongoTable.find({"_id": area})[0]['building']
            data = self.mongoTable.find({"_id": area})[0]['demand']
            return data, tech
        except:
            return [], []