import pandas as pd
from matplotlib import pyplot as plt
import numpy as np
import multiprocessing
from joblib import Parallel, delayed

from interfaces.interface_mongo import mongoInterface
from interfaces.interface_Influx import InfluxInterface
from apps.misc_Holiday import getHolidays
from components.dem_Consumer import h0_model, g0_model, rlm_model
from aggregation.dem_Port import demPort

def getSolarPower(plz):

    mongoDB = mongoInterface(database='MAS_XXXX', area=plz)
    influxDB = InfluxInterface(host='149.201.88.150', database='MAS_2020')
    geos = pd.read_excel(r'./data/InfoGeo.xlsx', index_col=0)

    portfolio = demPort(typ="DEM")
    for key, value in mongoDB.getPVs().items():
       portfolio.addToPortfolio('Pv' + str(key), {'Pv' + str(key): value})

    listSolar = []
    days = pd.date_range(start='2019-01-01', freq='d', periods=365)
    for day in days:
        print(day)
        totalSolar = np.zeros(24)
        try:
            geo = geos.loc[geos['PLZ'] == plz, 'hash'].to_numpy()[0]
            weather = influxDB.get_weather(geo, day)
            # Standardoptimierung
            portfolio.setPara(day, weather, np.zeros(24))
            portfolio.buildModel()
            power_dayAhead = np.asarray(portfolio.optimize(), np.float)  # Berechnung der Einspeiseleitung [kW]
            for k in range(24):
                    totalSolar[k] += power_dayAhead[k]
        except Exception as e:
            # print(e)
            print('no plz-area')
        listSolar.append(totalSolar)

    return listSolar

if __name__ == "__main__":

    holidays = getHolidays(2019)

    df = pd.read_excel(r'C:\Users\Administrator\Desktop\dmas\model\data\Ref_Demand.xlsx', index_col=0)
    df.index = pd.date_range(start='2019-01-01', periods=35040, freq='15min')
    df = df.resample('h').mean()

    num_cores = min(multiprocessing.cpu_count(), 60)
    # test = getSolarPower(3)
    listSolar = Parallel(n_jobs=num_cores)(delayed(getSolarPower)(i) for i in range(1, 100))
    solar = np.sum(np.asarray([np.asarray(i).reshape((-1)) for i in listSolar]), axis=0) / 10 ** 6

    values = df.to_numpy()
    values = values.reshape((-1))/1000
    testh0 = h0_model()
    testg0 = g0_model()
    testRlm = rlm_model()

    iEnergy = 0
    gEnergy = 0
    hEnergy = 0

    for i in range(1, 100):
        mongoDB = mongoInterface(database='MAS_XXXX', area=i)
        try:
            demand = mongoDB.getDemand()
            hEnergy += demand['h0']
            gEnergy += demand['g0']
            iEnergy += demand['rlm']
        except:
            print('keine Lastdaten')

    iEnergy = dict(demandP=iEnergy * 10 ** 6)
    gEnergy = dict(demandP=gEnergy * 10 ** 6)
    hEnergy = dict(demandP=hEnergy * 10 ** 6 + np.sum(solar) * 10 ** 6)

    days = pd.date_range(start='2019-01-01', freq='d', periods=365)

    totalh0 = []
    totalg0 = []
    # totalRlm = []

    for day in days:
        totalh0.append(testh0.getPowerDemand(hEnergy, day))
        totalg0.append(testg0.getPowerDemand(gEnergy, day))
        # totalRlm.append(testRlm.getPowerDemand(iEnergy, day))
    #
    totalh0 = np.asarray(totalh0).reshape((-1))
    totalg0 = np.asarray(totalg0).reshape((-1))
    # totalRlm = np.asarray(totalRlm).reshape((-1))
    #
    totalh0PV = totalh0 / 10 ** 6 + solar
    #
    total = (totalh0PV + totalg0 / 10 ** 6)
    #
    # #plt.plot(totalRlm/10 ** 6)
    # #plt.plot(totalg0 / 10 ** 6)
    # #plt.plot(totalh0 / 10 ** 6)
    # plt.plot(values)
    #
    # sommer=np.asarray(np.load(open(r'./data/Time_Summer.array','rb')), np.int64)
    # winter = np.asarray(np.load(open(r'./data/Time_Winter.array', 'rb')), np.int64)
    # x = values-total
    # plt.plot(x)
    # rlm = pd.DataFrame(x)
    # rlm = rlm/np.sum(rlm)*1000000
    # rlm.index = pd.date_range(start='2019-01-01', periods=8760, freq='h')
    #
    # typDays = []
    #
    # result = rlm.loc[[i.dayofyear in winter for i in rlm.index]]
    # so = result.loc[[i.dayofweek == 6 or i in holidays[0] for i in result.index]]
    # so = so.groupby(so.index.hour).mean().to_numpy().reshape(-1)
    # sa = result.loc[[i.dayofweek == 5 and i not in holidays[0] for i in result.index]]
    # sa = sa.groupby(sa.index.hour).mean().to_numpy().reshape(-1)
    # wt = result.loc[[i.dayofweek in [0, 1, 2, 3, 4] and i not in holidays[0] for i in result.index]]
    # wt = wt.groupby(wt.index.hour).mean().to_numpy().reshape(-1)
    #
    # typDays.append([element for element in sa for _ in range(4)])
    # typDays.append([element for element in so for _ in range(4)])
    # typDays.append([element for element in wt for _ in range(4)])
    #
    # result = rlm.loc[[i.dayofyear in sommer for i in rlm.index]]
    # so = result.loc[[i.dayofweek == 6 or i in holidays[0] for i in result.index]]
    # so = so.groupby(so.index.hour).mean().to_numpy().reshape(-1)
    # sa = result.loc[[i.dayofweek == 5 and i not in holidays[0] for i in result.index]]
    # sa = sa.groupby(sa.index.hour).mean().to_numpy().reshape(-1)
    # wt = result.loc[[i.dayofweek in [0, 1, 2, 3, 4] and i not in holidays[0] for i in result.index]]
    # wt = wt.groupby(wt.index.hour).mean().to_numpy().reshape(-1)
    #
    # typDays.append([element for element in sa for _ in range(4)])
    # typDays.append([element for element in so for _ in range(4)])
    # typDays.append([element for element in wt for _ in range(4)])
    #
    # result = rlm.loc[[(i.dayofyear not in sommer) or i.dayofyear not in winter for i in rlm.index]]
    # so = result.loc[[i.dayofweek == 6 or i in holidays[0] for i in result.index]]
    # so = so.groupby(so.index.hour).mean().to_numpy().reshape(-1)
    # sa = result.loc[[i.dayofweek == 5 and i not in holidays[0] for i in result.index]]
    # sa = sa.groupby(sa.index.hour).mean().to_numpy().reshape(-1)
    # wt = result.loc[[i.dayofweek in [0, 1, 2, 3, 4] and i not in holidays[0] for i in result.index]]
    # wt = wt.groupby(wt.index.hour).mean().to_numpy().reshape(-1)
    #
    # typDays.append([element for element in sa for _ in range(4)])
    # typDays.append([element for element in so for _ in range(4)])
    # typDays.append([element for element in wt for _ in range(4)])
    #
    # outfile = open('./data/Ref_RLM.array','wb')
    #
    # typDays = np.asarray(typDays)
    # x = typDays.T
    # np.save(outfile, x)
    #
    # outfile.close()