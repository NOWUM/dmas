import os
os.chdir(os.path.dirname(os.path.dirname(__file__)))
import pandas as pd
import numpy as np
import pymongo
from windpowerlib.modelchain import ModelChain
from windpowerlib.wind_turbine import WindTurbine
from windpowerlib import wind_turbine as wt

def findEnerconTyp(df, number, typs, powers, mapping):

    totalDict = {}
    lst = []

    for typ in typs:
        try:
            if 'E' in typ and str(number) in typ:
                lst.append(typ)
        except:
            print('invalid typ')

    for typ in lst:
        dfTyp = df[df['Typenbezeichnung'] == typ]
        system = {}
        for row in dfTyp.iterrows():
            diff = np.abs(np.asarray(powers) - row[1].iloc[5])
            index = np.where(diff == diff.min())[0][-1]
            system.update({'manufacturer': mapping[row[1].iloc[2]],
                           'turbine_type': 'E' + str(number) + '/' + str(powers[index]),
                           'height': row[1].iloc[6],
                           'maxPower': row[1].iloc[5],
                           'plz': row[1].iloc[8]})
            totalDict.update({row[1].iloc[0]: system})

    return totalDict

def findVestasTyp(df, number, typs, powers, mapping):

    totalDict = {}
    lst = []

    for typ in typs:
        try:
            if 'V' in typ and str(number) in typ:
                lst.append(typ)
        except:
            print('invalid typ')

    for typ in lst:
        dfTyp = df[df['Typenbezeichnung'] == typ]
        system = {}
        for row in dfTyp.iterrows():
            diff = np.abs(np.asarray(powers) - row[1].iloc[5])
            index = np.where(diff == diff.min())[0][-1]
            system.update({'manufacturer': mapping[row[1].iloc[2]],
                           'turbine_type': 'V' + str(number) + '/' + str(powers[index]),
                           'height': row[1].iloc[6],
                           'maxPower': row[1].iloc[5],
                           'plz': row[1].iloc[8]})
            totalDict.update({row[1].iloc[0]: system})

    return totalDict

def findNordexTyp(df, number, typs, powers, mapping):

    totalDict = {}
    lst = []

    for typ in typs:
        try:
            if 'N' in typ and str(number) in typ:
                lst.append(typ)
        except:
            print('invalid typ')

    for typ in lst:
        dfTyp = df[df['Typenbezeichnung'] == typ]
        system = {}
        for row in dfTyp.iterrows():
            diff = np.abs(np.asarray(powers) - row[1].iloc[5])
            index = np.where(diff == diff.min())[0][-1]
            system.update({'manufacturer': mapping[row[1].iloc[2]],
                           'turbine_type': 'N' + str(number) + '/' + str(powers[index]),
                           'height': row[1].iloc[6],
                           'maxPower': row[1].iloc[5],
                           'plz': row[1].iloc[8]})
            totalDict.update({row[1].iloc[0]: system})

    return totalDict

def findSenvionTyp(df, number, typs, powers, mapping):

    totalDict = {}
    lst = []

    for typ in typs:
        try:
            if ('MM' in typ or 'S' in typ) and str(number) in typ:
                lst.append(typ)
        except:
            print('invalid typ')

    for typ in lst:
        dfTyp = df[df['Typenbezeichnung'] == typ]
        system = {}

        if 'MM' in typ:
            start = 'MM'
        else:
            start = 'S'

        for row in dfTyp.iterrows():
            diff = np.abs(np.asarray(powers) - row[1].iloc[5])
            index = np.where(diff == diff.min())[0][-1]
            system.update({'manufacturer': mapping[row[1].iloc[2]],
                           'turbine_type': start + str(number) + '/' + str(powers[index]),
                           'height': row[1].iloc[6],
                           'maxPower': row[1].iloc[5],
                           'plz': row[1].iloc[8]})
            totalDict.update({row[1].iloc[0]: system})

    return totalDict

def findEnoTyp(df, number, typs, powers, mapping):

    totalDict = {}
    lst = []

    for typ in typs:
        try:
            if 'eno' in typ and str(number) in typ:
                lst.append(typ)
        except:
            print('invalid typ')

    for typ in lst:
        dfTyp = df[df['Typenbezeichnung'] == typ]
        system = {}
        for row in dfTyp.iterrows():
            diff = np.abs(np.asarray(powers) - row[1].iloc[5])
            index = np.where(diff == diff.min())[0][-1]
            system.update({'manufacturer': mapping[row[1].iloc[2]],
                           'turbine_type': 'ENO' + str(number) + '/' + str(powers[index]),
                           'height': row[1].iloc[6],
                           'maxPower': row[1].iloc[5],
                           'plz': row[1].iloc[8]})
            totalDict.update({row[1].iloc[0]: system})

    return totalDict

def findGETyp(df, number, typs, powers, mapping):

    totalDict = {}
    lst = []

    for typ in typs:
        try:
            if 'GE' in typ and str(number) in typ:
                lst.append(typ)
        except:
            print('invalid typ')

    for typ in lst:
        dfTyp = df[df['Typenbezeichnung'] == typ]
        system = {}
        for row in dfTyp.iterrows():
            diff = np.abs(np.asarray(powers) - row[1].iloc[5])
            index = np.where(diff == diff.min())[0][-1]
            system.update({'manufacturer': mapping[row[1].iloc[2]],
                           'turbine_type': 'GE' + str(number) + '/' + str(powers[index]),
                           'height': row[1].iloc[6],
                           'maxPower': row[1].iloc[5],
                           'plz': row[1].iloc[8]})
            totalDict.update({row[1].iloc[0]: system})

    return totalDict

def findSiemensTyp(df, number, typs, powers, mapping):

    totalDict = {}
    lst = []

    for typ in typs:
        try:
            if 'SWT' in typ and str(number) in typ:
                lst.append(typ)
        except:
            print('invalid typ')

    for typ in lst:
        dfTyp = df[df['Typenbezeichnung'] == typ]
        system = {}
        for row in dfTyp.iterrows():
            diff = np.abs(np.asarray(powers) - row[1].iloc[5])
            index = np.where(diff == diff.min())[0][-1]
            system.update({'manufacturer': mapping[row[1].iloc[2]],
                           'turbine_type': 'SWT' + str(number) + '/' + str(powers[index]),
                           'height': row[1].iloc[6],
                           'maxPower': row[1].iloc[5],
                           'plz': row[1].iloc[8]})
            totalDict.update({row[1].iloc[0]: system})

    return totalDict

def setDefaultTyp(df, typ, com):
    totalDict = {}
    system = {}
    for row in df.iterrows():
        system.update({'manufacturer': com,
                       'turbine_type': typ,
                       'height': row[1].iloc[6],
                       'maxPower': row[1].iloc[5],
                       'plz': row[1].iloc[8]})
        totalDict.update({row[1].iloc[0]: system})

    return totalDict

if __name__ == '__main__':

    onshore = True
    offhsore = True

    if onshore:

        df = pd.read_excel(r'./data/toBuildData/windOnshore.xlsx', index_col=0)
        company = df['HerstellerWindenergieanlageBezeichnung']
        company = company.unique()

        dfWindlib = wt.get_turbine_types(print_out=False)
        windlibCompany = dfWindlib['manufacturer'].unique()

        mapping = {     'ENERCON GmbH': 'Enercon',
                        'Vestas Deutschland GmbH': 'Vestas',
                        'Nordex SE': 'Nordex',
                        'eno energy GmbH': 'Eno',
                        'GE Wind Energy GmbH': 'GE Wind',
                        'VENTEGO AG': 'Vestas',
                        'Fuhrländer AG': 'notUsed',
                        'Senvion Deutschland GmbH': 'Senvion/REpower',
                        'Gamesa Corporación Tecnológica S.A.': 'Siemens',
                        'VENSYS Energy AG': 'Vensys',
                        'Siemens Wind Power GmbH & Co. KG': 'Siemens',
                        'Nordex Energy GmbH': 'Nordex',
                        'EVIAG AG': 'notUsed',
                        'Schütz GmbH & Co. KGaA': 'notUsed',
                        'Kenersys Europe GmbH': 'notUsed',
                        'Adwen GmbH': 'Adwen/Areva',
                        'PowerWind GmbH': 'notUsed',
                        'Amperax Energie GmbH': 'notUsed',
                        'QREON GmbH': 'notUsed',
                        'FuSystems SkyWind GmbH': 'notUsed',
                        'FWT energy GmbH': 'notUsed',
                        'AREVA GmbH': 'Adwen/Areva'
        }

        allTurbines = {}

        # Build Enercon Windturbines
        enercon = df[df['HerstellerWindenergieanlageBezeichnung'] == 'ENERCON GmbH']
        enerconWindLib = dfWindlib[dfWindlib['manufacturer'] == mapping['ENERCON GmbH']]
        allTurbines.update(findEnerconTyp(enercon, 101, enercon['Typenbezeichnung'].unique(), powers=[3050, 3500], mapping=mapping))
        allTurbines.update(findEnerconTyp(enercon, 115, enercon['Typenbezeichnung'].unique(), powers=[3000, 3200], mapping=mapping))
        allTurbines.update(findEnerconTyp(enercon, 126, enercon['Typenbezeichnung'].unique(), powers=[4200, 7500, 7580], mapping=mapping))
        allTurbines.update(findEnerconTyp(enercon, 141, enercon['Typenbezeichnung'].unique(), powers=[4200], mapping=mapping))
        allTurbines.update(findEnerconTyp(enercon, 53, enercon['Typenbezeichnung'].unique(), powers=[800], mapping=mapping))
        allTurbines.update(findEnerconTyp(enercon, 70, enercon['Typenbezeichnung'].unique(), powers=[2000, 2300], mapping=mapping))
        allTurbines.update(findEnerconTyp(enercon, 82, enercon['Typenbezeichnung'].unique(), powers=[2000, 2300, 2350, 3000], mapping=mapping))
        allTurbines.update(findEnerconTyp(enercon, 92, enercon['Typenbezeichnung'].unique(), powers=[2350], mapping=mapping))
        allTurbines.update(findEnerconTyp(enercon, 48, enercon['Typenbezeichnung'].unique(), powers=[800], mapping=mapping))

        # Build Vestas Windturbines
        vestas = df[df['HerstellerWindenergieanlageBezeichnung'] == 'Vestas Deutschland GmbH']
        vestasWindLib = dfWindlib[dfWindlib['manufacturer'] == mapping['Vestas Deutschland GmbH']]
        allTurbines.update(findVestasTyp(vestas, 100, vestas['Typenbezeichnung'].unique(), powers=[1800], mapping=mapping))
        allTurbines.update(findVestasTyp(vestas, 112, vestas['Typenbezeichnung'].unique(), powers=[3000, 3075, 3300, 3450], mapping=mapping))
        allTurbines.update(findVestasTyp(vestas, 117, vestas['Typenbezeichnung'].unique(), powers=[3300, 3450, 3600], mapping=mapping))
        allTurbines.update(findVestasTyp(vestas, 126, vestas['Typenbezeichnung'].unique(), powers=[3000, 3300, 3450], mapping=mapping))
        allTurbines.update(findVestasTyp(vestas, 80, vestas['Typenbezeichnung'].unique(), powers=[2000], mapping=mapping))
        allTurbines.update(findVestasTyp(vestas, 90, vestas['Typenbezeichnung'].unique(), powers=[2000, 3000], mapping=mapping))

        # Build Nordex Windturbines
        nordex = df[df['HerstellerWindenergieanlageBezeichnung'] == 'Nordex SE']
        nordexWindLib = dfWindlib[dfWindlib['manufacturer'] == mapping['Nordex SE']]
        allTurbines.update(findNordexTyp(nordex, 90, nordex['Typenbezeichnung'].unique(), powers=[2500], mapping=mapping))
        allTurbines.update(findNordexTyp(nordex, 100, nordex['Typenbezeichnung'].unique(), powers=[2500], mapping=mapping))
        allTurbines.update(findNordexTyp(nordex, 117, nordex['Typenbezeichnung'].unique(), powers=[2400], mapping=mapping))
        allTurbines.update(findNordexTyp(nordex, 131, nordex['Typenbezeichnung'].unique(), powers=[3000, 3300, 3600], mapping=mapping))

        # Build Senvion Windturbines
        senvion = df[df['HerstellerWindenergieanlageBezeichnung'] == 'Senvion Deutschland GmbH']
        senvionWindLib = dfWindlib[dfWindlib['manufacturer'] == mapping['Senvion Deutschland GmbH']]
        allTurbines.update(findSenvionTyp(senvion, 100, senvion['Typenbezeichnung'].unique(), powers=[2000], mapping=mapping))
        allTurbines.update(findSenvionTyp(senvion, 92, senvion['Typenbezeichnung'].unique(), powers=[2050], mapping=mapping))
        allTurbines.update(findSenvionTyp(senvion, 104, senvion['Typenbezeichnung'].unique(), powers=[3400], mapping=mapping))
        allTurbines.update(findSenvionTyp(senvion, 114, senvion['Typenbezeichnung'].unique(), powers=[3200, 3400], mapping=mapping))
        allTurbines.update(findSenvionTyp(senvion, 122, senvion['Typenbezeichnung'].unique(), powers=[3000, 3200], mapping=mapping))
        allTurbines.update(findSenvionTyp(senvion, 126, senvion['Typenbezeichnung'].unique(), powers=[6150], mapping=mapping))
        allTurbines.update(findSenvionTyp(senvion, 152, senvion['Typenbezeichnung'].unique(), powers=[6330], mapping=mapping))

        # Build Eno Windturbines
        eno = df[df['HerstellerWindenergieanlageBezeichnung'] == 'eno energy GmbH']
        enoWindLib = dfWindlib[dfWindlib['manufacturer'] == mapping['eno energy GmbH']]
        allTurbines.update(findEnoTyp(eno, 100, eno['Typenbezeichnung'].unique(), powers=[2200], mapping=mapping))
        allTurbines.update(findEnoTyp(eno, 114, eno['Typenbezeichnung'].unique(), powers=[3500], mapping=mapping))
        allTurbines.update(findEnoTyp(eno, 126, eno['Typenbezeichnung'].unique(), powers=[3500], mapping=mapping))

        # Build GE Wind Windturbines
        ge = df[df['HerstellerWindenergieanlageBezeichnung'] == 'GE Wind Energy GmbH']
        geWindLib = dfWindlib[dfWindlib['manufacturer'] == mapping['GE Wind Energy GmbH']]
        allTurbines.update(findGETyp(ge, 100, ge['Typenbezeichnung'].unique(), powers=[2500], mapping=mapping))
        allTurbines.update(findGETyp(ge, 103, ge['Typenbezeichnung'].unique(), powers=[2750], mapping=mapping))
        allTurbines.update(findGETyp(ge, 120, ge['Typenbezeichnung'].unique(), powers=[2500, 2750], mapping=mapping))
        allTurbines.update(findGETyp(ge, 130, ge['Typenbezeichnung'].unique(), powers=[3200], mapping=mapping))

        # Build Ventego Windturbines
        ventego = df[df['HerstellerWindenergieanlageBezeichnung'] == 'VENTEGO AG']
        ventegoWindLib = dfWindlib[dfWindlib['manufacturer'] == mapping['VENTEGO AG']]
        allTurbines.update(findVestasTyp(ventego, 100, ventego['Typenbezeichnung'].unique(), powers=[1800], mapping=mapping))
        allTurbines.update(findVestasTyp(ventego, 112, ventego['Typenbezeichnung'].unique(), powers=[3000, 3075, 3300, 3450], mapping=mapping))
        allTurbines.update(findVestasTyp(ventego, 117, ventego['Typenbezeichnung'].unique(), powers=[3300, 3450, 3600], mapping=mapping))
        allTurbines.update(findVestasTyp(ventego, 126, ventego['Typenbezeichnung'].unique(), powers=[3000, 3300, 3450], mapping=mapping))
        allTurbines.update(findVestasTyp(ventego, 80, ventego['Typenbezeichnung'].unique(), powers=[2000], mapping=mapping))
        allTurbines.update(findVestasTyp(ventego, 90, ventego['Typenbezeichnung'].unique(), powers=[2000, 3000], mapping=mapping))

        # Build Siemens Windturbines
        siemens = df[df['HerstellerWindenergieanlageBezeichnung'] == 'Siemens Wind Power GmbH & Co. KG']
        siemensWindLib = dfWindlib[dfWindlib['manufacturer'] == mapping['Siemens Wind Power GmbH & Co. KG']]
        allTurbines.update(findSiemensTyp(siemens, 113, siemens['Typenbezeichnung'].unique(), powers=[2300, 3200], mapping=mapping))
        allTurbines.update(findSiemensTyp(siemens, 120, siemens['Typenbezeichnung'].unique(), powers=[3600], mapping=mapping))
        allTurbines.update(findSiemensTyp(siemens, 130, siemens['Typenbezeichnung'].unique(), powers=[3300, 3600], mapping=mapping))
        allTurbines.update(findSiemensTyp(siemens, 142, siemens['Typenbezeichnung'].unique(), powers=[3150], mapping=mapping))

        notMan = df[pd.isna(df['HerstellerWindenergieanlageBezeichnung'])]
        allTurbines.update(setDefaultTyp(notMan[notMan['Nettonennleistung'] == 1000], 'AN/1000', 'AN_Bonus'))
        allTurbines.update(setDefaultTyp(notMan[notMan['Nettonennleistung'] == 1300], 'AN/1300', 'AN_Bonus'))
        allTurbines.update(setDefaultTyp(notMan[notMan['Nettonennleistung'] == 1500], 'E-66/1500', 'Enercon'))
        allTurbines.update(setDefaultTyp(notMan[notMan['Nettonennleistung'] == 1800], 'V66/1650', 'Vestas'))
        allTurbines.update(setDefaultTyp(notMan[notMan['Nettonennleistung'] == 1800], 'V100/1800', 'Vestas'))
        allTurbines.update(setDefaultTyp(notMan[notMan['Nettonennleistung'] == 2000], 'V90/2000', 'Vestas'))
        allTurbines.update(setDefaultTyp(notMan[notMan['Nettonennleistung'] == 2300], 'E-82/2300', 'Enercon'))
        allTurbines.update(setDefaultTyp(notMan[notMan['Nettonennleistung'] == 2500], 'N100/2500', 'Nordex'))
        allTurbines.update(setDefaultTyp(notMan[notMan['Nettonennleistung'] == 3000], 'E-115/3000', 'Enercon'))
        allTurbines.update(setDefaultTyp(notMan[notMan['Nettonennleistung'] == 3050], 'E-101/3050', 'Enercon'))
        allTurbines.update(setDefaultTyp(notMan[notMan['Nettonennleistung'] == 3300], 'V112/3300', 'Vestas'))

        keys = np.asarray([key for key in allTurbines.keys()])
        ids = df['Id'].to_numpy()
        notUsed = np.setdiff1d(ids,keys)

        for i in notUsed:
            element = df[df['Id'] == i]
            system = {}
            system.update({'manufacturer': 'Enercon',
                           'turbine_type': 'E-115/3000',
                           'height': element.iloc[0, 6],
                           'maxPower': 3000,
                           'plz': element.iloc[0, 8]})
            allTurbines.update({i: system})

        totalPower = 0
        for key, value in allTurbines.items():
           totalPower += value['maxPower']
        # print(totalPower/10**6)

        areas = [[] for _ in range(1, 100)]
        counter = 0

        for key, value in allTurbines.items():
            if len(str(value['plz'])) == 1:
                print('invalid plz ' + str(value['plz']))
            elif len(str(value['plz'])) == 4:
                index = '0' + str(value['plz'])[0]
                areas[int(index)-1].append({key: value})
                counter += 1
            else:
                index = str(value['plz'])[:2]
                areas[int(index)-1].append({key: value})
                counter += 1

        mongo = pymongo.MongoClient('mongodb://' + '149.201.88.150' + ':27017/')
        structDB = mongo["testDB"]

        for i in range(99):
            tableStructur = structDB["PLZ_%s" % str(i+1)]
            query = {"_id": 'WindSystems'}
            tableStructur.delete_one(query)
            dict_ = {}
            for element in areas[i]:
                nameCounter = 0
                for key, value in element.items():
                    value.update({'plz': int(value['plz'])})
                    value.update({'land': True})
                    dict_.update({'plz' + str(i+1) + 'systemOn' + str(nameCounter): value})
                    nameCounter += 1
            query = {"_id": 'WindSystems'}
            d = {"$set": dict_}
            try:
                tableStructur.update_one(filter=query, update=d, upsert=True)
            except Exception as e:
                print(e)

    if offhsore:
        df = pd.read_excel(r'./data/toBuildData/windOffshore.xlsx', index_col=0)

        df18 = df[df['Plz'] == 18000]
        df26 = df[df['Plz'] == 26000]

        mongo = pymongo.MongoClient('mongodb://' + '149.201.88.150' + ':27017/')
        structDB = mongo["testDB"]

        dict_ = {}
        nameCounter = 0
        tableStructur = structDB["PLZ_18"]
        for element in df18.iterrows():
            value = {   'manufacturer' : element[1].iloc[2],
                        'turbine_tpye': element[1].iloc[4],
                        'height': element[1].iloc[6],
                        'maxPower': element[1].iloc[5],
                        'plz': element[1].iloc[8],
                        'land': False}
            dict_.update({'plz' + str(18) + 'systemOff' + str(nameCounter): value})
            nameCounter += 1

        query = {"_id": 'WindSystems'}
        d = {"$set": dict_}
        tableStructur.update_one(filter=query, update=d, upsert=True)

        tableStructur = structDB["PLZ_26"]
        dict_ = {}
        nameCounter = 0
        for element in df26.iterrows():
            value = {   'manufacturer' : element[1].iloc[2],
                        'turbine_tpye': element[1].iloc[4],
                        'height': element[1].iloc[6],
                        'maxPower': element[1].iloc[5],
                        'plz': element[1].iloc[8],
                        'land': False}
            dict_.update({'plz' + str(26) + 'systemOff' + str(nameCounter): value})
            nameCounter += 1

        query = {"_id": 'WindSystems'}
        d = {"$set": dict_}
        tableStructur.update_one(filter=query, update=d, upsert=True)