import sys
import requests
import pandas as pd
import numpy as np
import time
from datetime import datetime
import matplotlib as mpl
mpl.use('QT5Agg')
from matplotlib import pyplot as plt
import json
import pymongo

def get_wind_turbine_systems(bool_netzbetreiberpruefung_must_be_checked=False):
    """
    function reads wind turbine data from "www.marktstammdatenregister.de"
    and writes it to "Windkraftanlagen.xlsx".

    :param bool_netzbetreiberpruefung_must_be_checked: "True" if the status of "Netzbetreiberprüfung" must be "Geprueft"
                                                       , "False" otherwise.
    :return: nothing, but creates a ".xlsx"-file
    """

    url = ''

    # get filtered data from website (here just the first entry, for all data see below)
    if bool(bool_netzbetreiberpruefung_must_be_checked):
        url = 'https://www.marktstammdatenregister.de/MaStR/Einheit/EinheitJson/' \
              'GetErweiterteOeffentlicheEinheitStromerzeugung?sort=IsNBPruefungAbgeschlossen-asc&page=1&pageSize=1&' \
              'group=&filter=Netzbetreiberpr%C3%BCfung~eq~%272954%27~and~Energietr%C3%A4ger~eq~%272497%27'
    else:
        url = 'https://www.marktstammdatenregister.de/MaStR/Einheit/EinheitJson/' \
              'GetErweiterteOeffentlicheEinheitStromerzeugung?sort=&page=1&pageSize=1&' \
              'group=&filter=Energietr%C3%A4ger~eq~%272497%27'

    # the other way to filter "Netzbetreiberpruefung" == "Geprueft" is to get all data from website and check
    # the field "IsNBPruefungAbgeschlossen" in entries. Value 2954 is "Geprueft", value 2955 stands for "In Pruefung".

    resp = requests.get(url=url)
    data_raw = resp.json()
    total = data_raw.get('Total')
    resp.close()

    keys_wanted = ['Id', 'MaStRNummer', 'HerstellerWindenergieanlageBezeichnung', 'Typenbezeichnung',
                   'Nettonennleistung', 'NabenhoeheWindenergieanlage', 'RotordurchmesserWindenergieanlage',
                   'Plz', 'Breitengrad', 'Laengengrad', 'LageEinheitBezeichnung',
                   'BetriebsStatusName', 'InbetriebnahmeDatum', 'EndgueltigeStilllegungDatum',
                   'IsNBPruefungAbgeschlossen']

    # Keys whose values​have to be formatted (if they exist) -> dates
    keys_4dates = ['InbetriebnahmeDatum', 'EndgueltigeStilllegungDatum']

    nb_pruefung_key = "IsNBPruefungAbgeschlossen"
    nb_pruefung_dict = {2954: "Geprueft", 2955: "In Pruefung"}

    data = []
    # max nr of rows per request = 25000
    max_entries = 25000.

    for i in range(1, int(round((total / max_entries) + 0.5)) + 1):

        # pageSize must be "total", not "maxEntries"
        url = ''
        # choose right url to get website filtered data
        if bool(bool_netzbetreiberpruefung_must_be_checked):
            url = 'https://www.marktstammdatenregister.de/MaStR/Einheit/EinheitJson/' \
                  'GetErweiterteOeffentlicheEinheitStromerzeugung?sort=IsNBPruefungAbgeschlossen-asc&page=' + str(i) + \
                  '&pageSize=' + str(total) + \
                  '&group=&filter=Netzbetreiberpr%C3%BCfung~eq~%272954%27~and~Energietr%C3%A4ger~eq~%272497%27'
        else:
            url = 'https://www.marktstammdatenregister.de/MaStR/Einheit/EinheitJson/' \
                  'GetErweiterteOeffentlicheEinheitStromerzeugung?sort=&page=' + str(i) + \
                  '&pageSize=' + str(total) + '&group=&filter=Energietr%C3%A4ger~eq~%272497%27'

        resp = requests.get(url=url)
        data_raw = resp.json()
        data = data + data_raw.get('Data')
        resp.close()

    data_dict = {}

    # List of dicts to dict of dicts:
    # data_dict = {i: data[i] for i in range(len(data))}

    # filter (and sort) fields of "data" with wanted keys and paste the result into dict "data_dict"
    for i in range(len(data)):
        # format dates e.g. from " '/Date(1356562800000)/' " to "2012-12-27"
        for j in range(len(keys_4dates)):
            if data[i][keys_4dates[j]] is not None:
                data[i][keys_4dates[j]] = datetime.fromtimestamp(
                    int(data[i][keys_4dates[j]].split('(')[1].split(')')[0]) / 1000).date()
        data_dict[i] = {key: data[i][key] for key in keys_wanted}

    df = pd.DataFrame.from_dict(data_dict, orient='index')
    # replaces in column 'IsNBPruefungAbgeschlossen' (website: "Netzbetreiberpruefung") the integer code in its meaning
    df = df.replace({nb_pruefung_key: nb_pruefung_dict})

    # workingDir: os.getcwd()
    df.to_excel('Windkraftanlagen.xlsx')

def get_solar_systems():
    """
    function reads wind turbine data from "www.marktstammdatenregister.de"
    :return: pandas dataframe
    """
    data_dict = {}
    index = 0

    for x in range(1, 100):

        if x < 10:
            plz = '0' + str(x)
        else:
            plz = str(x)

        print('PLZ: ' + plz)
        url = 'https://www.marktstammdatenregister.de/MaStR/Einheit/EinheitJson/' \
              'GetErweiterteOeffentlicheEinheitStromerzeugung?sort=&page=1&pageSize=1&group=&' \
              'filter=Postleitzahl~sw~%27' + plz + '%27~and~Lage%20der%20Einheit~neq~%27852%27~and~Energietr%C3%A4ger~eq~%272495%27'

        resp = requests.get(url=url)
        data_raw = resp.json()
        total = data_raw.get('Total')
        resp.close()

        keys_wanted = ['Id', 'MaStRNummer', 'Nettonennleistung',
                       'Plz', 'Breitengrad', 'Laengengrad', 'LageEinheitBezeichnung',
                       'BetriebsStatusName', 'InbetriebnahmeDatum', 'EndgueltigeStilllegungDatum',
                       'IsNBPruefungAbgeschlossen']

        # Keys whose values​have to be formatted (if they exist) -> dates
        keys_4dates = ['InbetriebnahmeDatum', 'EndgueltigeStilllegungDatum']

        nb_pruefung_key = "IsNBPruefungAbgeschlossen"
        nb_pruefung_dict = {2954: "Geprueft", 2955: "In Pruefung"}

        data = []
        # max nr of rows per request = 25000
        max_entries = 25000.

        for i in range(1, int(round((total / max_entries) + 0.5)) + 1):

            url = 'https://www.marktstammdatenregister.de/MaStR/Einheit/EinheitJson/' \
                  'GetErweiterteOeffentlicheEinheitStromerzeugung?sort=&page=' + str(i) + '&pageSize=' + str(total) + '&group=&' \
                  'filter=Postleitzahl~sw~%27'+ plz +'%27~and~Lage%20der%20Einheit~neq~%27852%27~and~Energietr%C3%A4ger~eq~%272495%27'

            resp = requests.get(url=url)
            data_raw = resp.json()
            data = data + data_raw.get('Data')
            resp.close()

        # filter (and sort) fields of "data" with wanted keys and paste the result into dict "data_dict"
        for i in range(len(data)):
            # format dates e.g. from " '/Date(1356562800000)/' " to "2012-12-27"
            for j in range(len(keys_4dates)):
                if data[i][keys_4dates[j]] is not None:
                    try:
                        data[i][keys_4dates[j]] = datetime.fromtimestamp(
                            int(data[i][keys_4dates[j]].split('(')[1].split(')')[0]) / 1000).date()
                    except:
                        print('invalid format')
            data_dict[index] = {key: data[i][key] for key in keys_wanted}
            index += 1

    df = pd.DataFrame.from_dict(data_dict, orient='index')
    df = df.replace({nb_pruefung_key: nb_pruefung_dict})

    return df

def get_solar_parks():
    """
    function reads wind turbine data from "www.marktstammdatenregister.de"
    :return: pandas dataframe
    """
    data_dict = {}
    index = 0
    for x in range(1, 100):

        if x < 10:
            plz = '0' + str(x)
        else:
            plz = str(x)

        print('PLZ: ' + plz)
        url = 'https://www.marktstammdatenregister.de/MaStR/Einheit/EinheitJson/' \
              'GetErweiterteOeffentlicheEinheitStromerzeugung?sort=&page=1&pageSize=1&group=&' \
              'filter=Postleitzahl~sw~%27' + plz + '%27~and~Lage%20der%20Einheit~eq~%27852%27~and~Energietr%C3%A4ger~eq~%272495%27'

        resp = requests.get(url=url)
        data_raw = resp.json()
        total = data_raw.get('Total')
        resp.close()

        keys_wanted = ['Id', 'MaStRNummer', 'Nettonennleistung',
                       'Plz', 'Breitengrad', 'Laengengrad', 'LageEinheitBezeichnung',
                       'BetriebsStatusName', 'InbetriebnahmeDatum', 'EndgueltigeStilllegungDatum',
                       'IsNBPruefungAbgeschlossen']

        # Keys whose values​have to be formatted (if they exist) -> dates
        keys_4dates = ['InbetriebnahmeDatum', 'EndgueltigeStilllegungDatum']

        nb_pruefung_key = "IsNBPruefungAbgeschlossen"
        nb_pruefung_dict = {2954: "Geprueft", 2955: "In Pruefung"}

        data = []
        # max nr of rows per request = 25000
        max_entries = 25000.

        for i in range(1, int(round((total / max_entries) + 0.5)) + 1):

            url = 'https://www.marktstammdatenregister.de/MaStR/Einheit/EinheitJson/' \
                  'GetErweiterteOeffentlicheEinheitStromerzeugung?sort=&page=' + str(i) + '&pageSize=' + str(total) + '&group=&' \
                  'filter=Postleitzahl~sw~%27'+ plz +'%27~and~Lage%20der%20Einheit~eq~%27852%27~and~Energietr%C3%A4ger~eq~%272495%27'

            resp = requests.get(url=url)
            data_raw = resp.json()
            data = data + data_raw.get('Data')
            resp.close()

        # filter (and sort) fields of "data" with wanted keys and paste the result into dict "data_dict"
        for i in range(len(data)):
            # format dates e.g. from " '/Date(1356562800000)/' " to "2012-12-27"
            for j in range(len(keys_4dates)):
                if data[i][keys_4dates[j]] is not None:
                    try:
                        data[i][keys_4dates[j]] = datetime.fromtimestamp(
                            int(data[i][keys_4dates[j]].split('(')[1].split(')')[0]) / 1000).date()
                    except:
                        print('invalid format')
            data_dict[index] = {key: data[i][key] for key in keys_wanted}
            index += 1

    df = pd.DataFrame.from_dict(data_dict, orient='index')
    df = df.replace({nb_pruefung_key: nb_pruefung_dict})

    return df

def initSystems():

    systems = {}
    for i in range(27):

        k = 0
        pos1 = 0
        pos2 = 0
        pos3 = 0

        while k < i:
            if pos1 < 2:
                pos1 += 1
                k += 1
            elif pos2 < 2:
                pos2 += 1
                k += 1
                pos1 = 0
            elif pos3 < 2:
                pos3 += 1
                k += 1
                pos2 = 0
                pos1 = 0

        systems.update({(pos1, pos2, pos3): 0})

    return systems

def buildSoloPV(tmp, analyse=False):

    if analyse:
        print(np.sum(tmp['Nettonennleistung'].to_numpy()))          # total capacity installed
        histData = plt.hist(tmp['Nettonennleistung'].to_numpy(), 11)
        plt.show()

    # split in three categories (small, medium, large)
    smallPV = tmp[tmp['Nettonennleistung'] <= 4]
    smallPVMean = np.mean(smallPV['Nettonennleistung'])
    smallPVProb = len(smallPV)/len(tmp)

    mediumPV = tmp[np.logical_and(tmp['Nettonennleistung'] > 4, tmp['Nettonennleistung'] <= 8)]
    mediumPVMean = np.mean(mediumPV['Nettonennleistung'])
    mediumPVProb = len(mediumPV)/len(tmp)

    largePV = tmp[tmp['Nettonennleistung'] > 8]
    largePVMean = np.mean(largePV['Nettonennleistung'])
    # largePVProb = len(largePV)/len(tmp)

    # count systems and EEG payed systems in each area
    dates = tmp['InbetriebnahmeDatum']
    lst = []
    for i in dates.index:
        try:
            if pd.to_datetime('2013-01-01') < datetime.fromtimestamp(dates[i]/1000):
                lst.append(True)
            else:
                lst.append(False)
        except:
            lst.append(False)
    EEGPayed = tmp.loc[lst, :]

    totalNumber = [0 for _ in range(100)]
    eegNumber = [0 for _ in range(100)]
    counter = 1
    for i in range(2000, 101000, 1000):
        totalNumber[counter] = len(tmp[tmp['Plz'] < i])
        eegNumber[counter] = len(EEGPayed[EEGPayed['Plz'] < i])
        counter += 1

    totalNumber = np.diff(np.asarray(totalNumber))
    eegNumber = np.diff(np.asarray(eegNumber))

    total_dict= {}
    for i in range(99):
        system = initSystems()
        for _ in range(totalNumber[i]):
            # ---------------------------------------------
            r1 = np.random.uniform()    # azimuth
            if r1 < 0.253:
                pos1 = 0
            elif r1 < 76.7:
                pos1 = 1
            else:
                pos1 = 2
            # ---------------------------------------------
            r1 = np.random.uniform()    # tilt
            if r1 < 0.03:
                pos2 = 0
            elif r1 < 0.94:
                pos2 = 1
            else:
                pos2 = 2
            # ---------------------------------------------
            r1 = np.random.uniform()    # power
            if r1 < smallPVProb:
                pos3 = 0
            elif r1 < smallPVProb + mediumPVProb:
                pos3 = 1
            else:
                pos3 = 2
            system.update({(pos1, pos2, pos3) : system[(pos1, pos2, pos3)] + 1})

        total_dict.update({i: system})

    to_save = {}
    azimuth = [110, 180, 250]
    tilt = [25, 35, 45]
    power = [np.round(smallPVMean, 1), np.round(mediumPVMean, 1), np.round(largePVMean, 1)]
    for index, dict_ in total_dict.items():
        factor = eegNumber[index]/totalNumber[index]
        if np.isnan(factor):
            factor = 0
        systems = {}
        for key, value in dict_.items():
            x = {'typ': 'Pv', 'para': [2.86, -37.18, 5.47, 0.16], 'demandP': np.round(1500*power[key[2]], 1), 'demandQ': 10000,
                 'PV': {'maxPower': power[key[2]], 'azimuth': azimuth[key[0]], 'tilt': tilt[key[1]]},
                 'num': value, 'EEG': int(value * factor)}
            systems.update({key: x})
        to_save.update({index+1: systems})

    return to_save, [smallPVProb, mediumPVProb], power

def buildBatPV(data, probs, powers):

    toPlot = []
    plz_s = []
    for system in data:
        for _, value in system.items():
            toPlot.append([value['PV']['maxPower'], value['Bat']['vmax'], value['Bat']['maxPower']])
            plz_s.append(value['plz'])
    toPlot = np.asarray(toPlot)

    dfPlz = pd.DataFrame(data = plz_s, columns=['Plz'])
    totalNumber = [0 for _ in range(100)]
    counter = 1
    for i in range(2000, 101000, 1000):
        totalNumber[counter] = len(dfPlz[dfPlz['Plz'] < i])
        counter += 1
    totalNumber = np.diff(np.asarray(totalNumber))

    df = pd.DataFrame(data=toPlot, columns=['kW PV', 'kWh BAT', 'kWBAT'])
    tmp = df[np.logical_and(df['kW PV'] <= 10, df['kW PV'] >= 1)]

    smallPV = tmp[tmp['kW PV'] <= 4]
    smallMean = np.round(np.mean(smallPV.loc[smallPV['kWh BAT'] <= 10, 'kWh BAT'].to_numpy()), 0)
    mediumPV = tmp[np.logical_and(tmp['kW PV'] > 4, tmp['kW PV'] <= 8)]
    mediumMean = np.round(np.mean(mediumPV.loc[mediumPV['kWh BAT'] <= 10, 'kWh BAT'].to_numpy()), 0)
    largePV = tmp[tmp['kW PV'] > 8]
    largeMean = np.round(np.mean(largePV.loc[largePV['kWh BAT'] <= 10, 'kWh BAT'].to_numpy()), 0)

    total_dict= {}
    for i in range(99):
        system = initSystems()
        for _ in range(totalNumber[i]):
            # ---------------------------------------------
            r1 = np.random.uniform()    # azimuth
            if r1 < 0.253:
                pos1 = 0
            elif r1 < 76.7:
                pos1 = 1
            else:
                pos1 = 2
            # ---------------------------------------------
            r1 = np.random.uniform()    # tilt
            if r1 < 0.03:
                pos2 = 0
            elif r1 < 0.94:
                pos2 = 1
            else:
                pos2 = 2
            # ---------------------------------------------
            r1 = np.random.uniform()    # power
            if r1 < probs[0]:
                pos3 = 0
            elif r1 < probs[0] + probs[1]:
                pos3 = 1
            else:
                pos3 = 2
            system.update({(pos1, pos2, pos3) : system[(pos1, pos2, pos3)] + 1})

        total_dict.update({i: system})

    to_save = {}
    azimuth = [110, 180, 250]
    tilt = [25, 35, 45]
    power = powers
    bat = [smallMean, mediumMean, largeMean]

    for index, dict_ in total_dict.items():
        systems = {}
        for key, value in dict_.items():
            x = {'typ': 'PvBat', 'para': [2.86, -37.18, 5.47, 0.16], 'demandP': np.round(1500*power[key[2]], 1), 'demandQ': 10000,
                 'PV': {'maxPower': power[key[2]], 'azimuth': azimuth[key[0]], 'tilt': tilt[key[1]]},
                 'Bat': {'v0': 0, 'vmax': bat[key[2]], 'eta': 0.96, 'maxPower': bat[key[2]]},
                 'num': value, 'EEG': 0}
            systems.update({key: x})
        to_save.update({index+1: systems})

    return to_save


if __name__ == "__main__":

    # get_wind_turbine_systems(bool_netzbetreiberpruefung_must_be_checked=True)

    # df = get_solar_systems()
    # df = get_solar_parks()

    # dfSystems = pd.read_json(r'C:\Users\rieke\Desktop\dmas\model\apps\PVSystems.json')
    # # filter for all merged Systems (Starts with SME)
    # dfSystems = dfSystems.loc[[True if 'SME' in s else False for s in dfSystems['MaStRNummer'].to_numpy(str)], :]
    # dfSystems = dfSystems[dfSystems['Nettonennleistung'] > 10]
    # dfParks = pd.read_json(r'C:\Users\rieke\Desktop\dmas\model\apps\PVParks.json')
    # # filter for all merged Systems (Starts with SME)
    # dfParks = dfParks.loc[[True if 'SME' in s else False for s in dfParks['MaStRNummer'].to_numpy(str)], :]
    #
    # dataParks = []
    # dataSystems = []
    # for i in range(1000, 100000, 1000):
    #     dataSystems.append(dfSystems[np.logical_and(dfSystems['Plz'] >= i, dfSystems['Plz'] < i+1000)])
    #     dataParks.append(dfParks[np.logical_and(dfParks['Plz'] >= i, dfParks['Plz'] < i + 1000)])
    #
    mongo = pymongo.MongoClient('mongodb://' + '149.201.88.150' + ':27017/')
    structDB = mongo["testDB"]
    # index = 0
    # for plz in range(1, 100):
    #     dataPark = dataParks[index]
    #     dataSystem = dataSystems[index]
    #
    #     tableStructur = structDB["PLZ_%s" % plz]
    #
    #     query = {"_id": 'PVParkSystems'}
    #     tableStructur.delete_one(query)
    #     query = {"_id": 'PVCommercialSystems'}
    #     tableStructur.delete_one(query)
    #
    #     azimuth = 180
    #     tilt = 35
    #
    #     maxPowerPark = np.round(np.sum(dataPark['Nettonennleistung'].to_numpy()), 1)
    #     maxPowerCommercial = np.round(np.sum(dataSystem['Nettonennleistung'].to_numpy()), 1)
    #
    #     try:
    #         query = {"_id": 'PVParkSystems'}
    #         systems = {
    #             'plz' + str(plz) + 'Park': {'typ': 'PVPark', 'fuel': 'solar',
    #                                         'maxPower': maxPowerPark,
    #                                         'azimuth': azimuth,
    #                                         'tilt': tilt},
    #             'plz' + str(plz) + 'TrIn': {'typ': 'PVTrIn', 'fuel': 'solar',
    #                                         'maxPower': maxPowerCommercial,
    #                                         'azimuth': azimuth,
    #                                         'tilt': tilt}
    #         }
    #         d = {"$set": systems}
    #         tableStructur.update_one(filter=query, update=d, upsert=True)
    #     except:
    #         print('no data')
    #     index += 1

    # select roof-top systems with 1<=P<=10 kWp
    # tmp = df[np.logical_and(df['Nettonennleistung'] <= 10, df['Nettonennleistung'] >= 1)]
    # pvSolo, probs, powers = buildSoloPV(tmp=tmp)
    #
    # with open(r'C:\Users\rieke\Desktop\dmas\model\data\PVBat_Systems.json') as json_file:
    #     data = json.load(json_file)
    #
    # pvBat = buildBatPV(data, probs, powers)
    # df = pd.read_excel(r'C:\Users\rieke\Desktop\dmas\model\data\InfoGeo.xlsx', index_col=0)
    #
    # mongo = pymongo.MongoClient('mongodb://' + '149.201.88.150' + ':27017/')
    # structDB = mongo["testDB"]
    #
    # for plz, systems in pvSolo.items():
    #     tableStructur = structDB["PLZ_%s" % plz]
    #     dict_ = {}
    #     for key, system in systems.items():
    #         dict_.update({str(key):system})
    #     query = {"_id": 'PVSystems'}
    #     d = {"$set": dict_}
    #     tableStructur.update_one(filter=query, update=d, upsert=True)
    #
    # for plz, systems in pvBat.items():
    #     tableStructur = structDB["PLZ_%s" % plz]
    #     dict_ = {}
    #     for key, system in systems.items():
    #         dict_.update({str(key):system})
    #     query = {"_id": 'PVBatSystems'}
    #     d = {"$set": dict_}
    #     tableStructur.update_one(filter=query, update=d, upsert=True)
    #
    # for plz in range(1, 100):
    #     data = df[df['PLZ'] == plz]
    #     tableStructur = structDB["PLZ_%s" % plz]
    #     try:
    #         lat = data['Latitude'].to_numpy()[0]
    #         lon = data['Longitude'].to_numpy()[0]
    #         hash = data['hash'].to_numpy()[0]
    #         dict_ = {'geohash': hash, 'lat': lat, 'lon': lon}
    #         query = {"_id": 'Position'}
    #         d = {"$set": dict_}
    #         tableStructur.update_one(filter=query, update=d, upsert=True)
    #     except:
    #         print('no plz area')
    #
    mongo = pymongo.MongoClient('mongodb://' + '149.201.88.150' + ':27017/')
    structDBOld = mongo["systemdata"]
    tableStructurOld = structDBOld["energysystems"]
    #
    for plz in range(1, 100):
        tableStructur = structDB["PLZ_%s" % plz]

        # --- Zusammenfassung Biomasseanlagen und schreiben in die neue DB
        writeBio = False
        if writeBio:
            query = {"_id": 'BiomassSystems'}
            tableStructur.delete_one(query)
            element = tableStructurOld.find_one({"_id": plz})['biomass']
            maxPower = 0
            if len(element) > 0:
                for _, value in element.items():
                    maxPower += value['maxPower']
            system = {'plz' + str(plz) + 'System' + str(0): {'typ': 'biomass', 'fuel': 'biomass',
                                                  'maxPower': maxPower}}
            query = {"_id": 'BiomassSystems'}
            d = {"$set": system}
            tableStructur.update_one(filter=query, update=d, upsert=True)

        # --- Zusammenfassung Laufwasserkraftwerke und schreiben in die neue DB
        writeWater = False
        if writeWater:
            element = tableStructurOld.find_one({"_id": plz})['run-river']
            query = {"_id": 'RunRiverSystems'}
            tableStructur.delete_one(query)
            maxPower = 0
            if len(element) > 0:
                for _, value in element.items():
                    maxPower += value['maxPower']
            system = {'plz' + str(plz) + 'System' + str(0): {'typ': 'run-river', 'fuel': 'water',
                                                  'maxPower': maxPower}}

            query = {"_id": 'RunRiverSystems'}
            d = {"$set": system}
            tableStructur.update_one(filter=query, update=d, upsert=True)


    #         element = tableStructurOld.find_one({"_id": plz})['storages']
    #         if len(element) > 0:
    #             query = {"_id": 'StorageSystems'}
    #             d = {"$set": element}
    #             tableStructur.update_one(filter=query, update=d, upsert=True)
    #
    #         element = tableStructurOld.find_one({"_id": plz})['powerPlants']
    #         if len(element) > 0:
    #             query = {"_id": 'PowerPlantSystems'}
    #             d = {"$set": element}
    #             tableStructur.update_one(filter=query, update=d, upsert=True)
    #
    #         element = tableStructurOld.find_one({"_id": plz})['demand']
    #         query = {"_id": 'ConsumerSystems'}
    #         d = {"$set": element}
    #         tableStructur.update_one(filter=query, update=d, upsert=True)