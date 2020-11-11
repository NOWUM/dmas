# third party modules
import pymongo
import socket


class mongoInterface:

    def __init__(self, database, area=1, host='149.201.88.150'):

        self.mongo = pymongo.MongoClient('mongodb://' + host + ':27017/')
        self.structDB = self.mongo["testDB"]
        self.tableStructur = self.structDB['PLZ_%s' %area]

        self.orderDB = self.mongo[database]
        self.status = self.orderDB['status']

    def get_position(self):
        try:
            position = self.tableStructur.find_one({'_id': 'Position'})
            return position
        except:
            return {}

    def get_power_plants(self):
        try:
            power_plants = self.tableStructur.find_one({"_id": 'PowerPlantSystems'})
            systems = {}
            for key, value in power_plants.items():
                if key != '_id':
                    systems.update({key:value})
            return systems
        except:
            return {}

    def get_storages(self):
        try:
            storages = self.tableStructur.find_one({"_id": 'StorageSystems'})
            systems = {}
            for key, value in storages.items():
                if key != '_id':
                    systems.update({key:value})
            return systems
        except:
            return {}

    def get_pv_batteries(self):
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

    def get_pvs(self):
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

    def get_demand(self):
        try:
            consumer = self.tableStructur.find_one({'_id': 'ConsumerSystems'})
            systems = {}
            for key, value in consumer.items():
                if key != '_id':
                    systems.update({key:value})
            return systems
        except:
            return {}

    def get_wind_turbines(self):
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

    def get_pv_parks(self):
        try:
            pv = self.tableStructur.find_one({'_id': 'PVParkSystems'})
            position = self.tableStructur.find_one({'_id': 'Position'})
            systems = {}
            for key, value in pv.items():
                if key != '_id':
                    value.update({'position': [position['lat'], position['lon']]})
                    systems.update({key: value})
            return systems
        except:
            return {}

    def get_runriver_systems(self):
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

    def get_biomass_systems(self):
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

    def get_market_status(self, date):
        try:
            clearing = self.orderDB[str(date.date())].find_one({'_id': 'market'})
            return clearing['DayAhead']
        except:
            return False

    def set_market_status(self, name, date):
        query = {"_id": name}
        clearing = {"$set": {"_id": name, "DayAhead": True}}
        self.orderDB[str(date.date())].update_one(filter=query, update=clearing, upsert=True)

    def login(self, name):
        hostname = socket.gethostname()  # get computer name
        ip_address = socket.gethostbyname(hostname)
        status = {
                    "_id": name,
                    "connected": True,
                    "ip": ip_address
                  }
        if self.status.find_one({"_id": name}) is None:
            self.status.insert_one(status)
        else:
            query = {"_id": name}
            status = {"$set": {"connected": True, "ip": ip_address}}
            self.status.update_one(query, status)

    def logout(self, name):
        query = {"_id": name}
        status = {"$set": {"connected": False, "ip": ""}}
        self.status.update_one(query, status)

    def set_dayAhead_orders(self, name, date, orders):
        query = {"_id": name}
        orders = {"$set": {"_id": name, "DayAhead": orders}}
        self.orderDB[str(date.date())].update_one(filter=query, update=orders, upsert=True)

    def get_agents(self):

        agents = {typ: 0 for typ in ['PWP', 'RES', 'DEM', 'STR', 'NET', 'MRK']}
        for id_ in self.status.find().distinct('_id'):
            dict_ = self.status.find_one({"_id": id_})
            typ = id_.split('_')[0]
            try:
                if dict_['connected']:
                    agents[typ] += 1
            except Exception as e:
                print(e)

        return agents

    def get_agents_ip(self, in_typ):

        agents = {}
        for id_ in self.status.find().distinct('_id'):
            dict_ = self.status.find_one({"_id": id_})
            typ = id_.split('_')[0]
            if typ == in_typ and dict_['connected']:
                agents.update({id_: dict_['ip']})

        return agents