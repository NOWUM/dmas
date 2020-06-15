import os
os.chdir(os.path.dirname(os.path.dirname(__file__)))
import numpy as np
from components.basic_EnergySystem import energySystem as es

class h0_model(es):
    """ Haushalte (H0) in einem Versorgungsgebiet """
    def __init__(self,
                 t=np.arange(24), T=24, dt=1,                                                       # Metainfo Zeit
                 demQ=1000, demP=3000,                                                              # Jahresverbräuche
                 refSLP=np.asarray(np.load(open(r'./data/Ref_H0.array','rb')), np.float32),         # SLP Strom
                 refTemp=np.asarray(np.load(open(r'./data/Ref_Temp.array', 'rb')), np.float32),     # Referenztemperatur
                 factors=np.asarray(np.load(open(r'./data/Ref_Factors.array', 'rb')), np.float32),  # Stundenfaktoren
                 parameters=np.asarray([2.8, -37, 5.4, 0.17], np.float32)):                         # Gebäudeparameter
        super().__init__(t, T, dt, demQ, demP, refSLP, refTemp, factors, parameters)

    def getPowerDemand(self, data, date):
        power = data['demandP'] * 0.2           # Variabler Anteil
        base = data['demandP'] * 0.8            # Konstanter Anteil
        # --> Anpassung Aufgrund der überschätzten Gleichzeitigkeit in den SLP
        # --> die Faktoren wurden empirisch ermittelt und sind eine grobe Schätzung

        demand = self.slpP.get_profile(date.dayofyear, date.dayofweek, power).reshape((96, 1))
        demand = np.asarray([np.mean(demand[i:i+3]) for i in range(0,96,4)], np.float).reshape((-1,)) + base / 8760

        return demand

    def build(self, data, ts, date):
        self.powerDemand = self.getPowerDemand(data, date)

class g0_model(es):
    """ Gewerbe (G0) in einem Versorgungsgebiet """
    def __init__(self,
                 t=np.arange(24), T=24, dt=1,                                                       # Metainfo Zeit
                 demQ=1000, demP=3000,                                                              # Jahresverbräuche
                 refSLP=np.asarray(np.load(open(r'./data/Ref_G0.array','rb')), np.float32),         # SLP Strom
                 refTemp=np.asarray(np.load(open(r'./data/Ref_Temp.array', 'rb')), np.float32),     # Referenztemperatur
                 factors=np.asarray(np.load(open(r'./data/Ref_Factors.array', 'rb')), np.float32),  # Stundenfaktoren
                 parameters=np.asarray([2.8, -37, 5.4, 0.17], np.float32)):                         # Gebäudeparameter
        super().__init__(t, T, dt, demQ, demP, refSLP, refTemp, factors, parameters, 1)

    def getPowerDemand(self, data, date):
        power = data['demandP'] * 0.2           # Variabler Anteil
        base = data['demandP'] * 0.8           # Konstanter Anteil
        # --> Anpassung Aufgrund der überschätzten Gleichzeitigkeit in den SLP
        # --> die Faktoren wurden empirisch ermittelt und sind eine grobe Schätzung

        demand = self.slpP.get_profile(date.dayofyear, date.dayofweek, power).reshape((96, 1))
        demand = np.asarray([np.mean(demand[i:i + 3]) for i in range(0, 96, 4)], np.float).reshape((-1,)) + base / 8760

        return demand

    def build(self, data, ts, date):
        self.powerDemand = self.getPowerDemand(data, date)

class rlm_model(es):
    """ Industrie (RLM) in einem Versorgungsgebiet """
    def __init__(self,
                 t=np.arange(24), T=24, dt=1,  # Metainfo Zeit
                 demQ=1000, demP=3000,  # Jahresverbräuche
                 refSLP=np.asarray(np.load(open(r'./data/Ref_RLM.array','rb')), np.float32),  # SLP Strom
                 refTemp=np.asarray(np.load(open(r'./data/Ref_Temp.array', 'rb')), np.float32),  # Referenztemperatur
                 factors=np.asarray(np.load(open(r'./data/Ref_Factors.array', 'rb')), np.float32),  # Stundenfaktoren
                 parameters=np.asarray([2.8, -37, 5.4, 0.17], np.float32)):                         # Gebäudeparameter
        super().__init__(t, T, dt, demQ, demP, refSLP, refTemp, factors, parameters, 1)

    def getPowerDemand(self, data, date):
        factor = 0.775                              # Korrektzur um Bahnnetz und Eigenerzeugung der Industrie
        power = data['demandP'] * 1 * factor        # Variabler Anteil
        base = data['demandP'] * 0 * factor          # Konstanter Anteil
        # --> Anpassung Aufgrund der überschätzten Gleichzeitigkeit in den SLP
        # --> die Faktoren wurden empirisch ermittelt und sind eine grobe Schätzung

        demand = self.slpP.get_profile(date.dayofyear, date.dayofweek, power).reshape((96, 1))
        demand = np.asarray([np.mean(demand[i:i + 3]) for i in range(0, 96, 4)], np.float).reshape((-1,)) + base / 8760

        return demand

    def build(self, data, ts, date):
        self.powerDemand = self.getPowerDemand(data, date)

if __name__ == "__main__":
    import pandas as pd
    from matplotlib import pyplot as plt
    import numpy as np

    from interfaces.interface_mongo import mongoInterface
    from apps.misc_Holiday import getHolidays

    holidays = getHolidays(2019)

    mongoDB = mongoInterface(database='MAS_XXXX')

    df = pd.read_excel(r'C:\Users\Administrator\Desktop\dmas\model\data\Ref_Demand.xlsx', index_col=0)
    df.index = pd.date_range(start='2019-01-01', periods=35040, freq='15min')
    df = df.resample('h').mean()

    values = df.to_numpy()
    values = values.reshape((-1))/1000
    testh0 = h0_model()
    testg0 = g0_model()
    testRlm = rlm_model()

    industrie=0
    gewerbe=0
    haushalte=0

    Anlagen = 0

    for i in range(1,100):
        try:
            #anz = mongoDB.getPvParks(i)
            #Anlagen += len(anz)
            #anz = mongoDB.getWindOn(i)
            #Anlagen += len(anz)
            demand = mongoDB.getDemand(i)
            haushalte += demand['h0']
            gewerbe += demand['g0']
            industrie += demand['rlm']# * 0.775
        except:
            print('keine Lastdaten')

    print(Anlagen)

    energy = gewerbe + haushalte + industrie
    energy /= 1000

    #tmp = industrie
    industrie = dict(demandP=industrie*10**6)
    gewerbe = dict(demandP=gewerbe*10**6)
    haushalte = dict(demandP=haushalte*10**6)

    days = pd.date_range(start='2019-01-01', freq='d',periods=365)

    totalh0 = []
    totalg0 = []
    totalRlm = []

    for day in days:
        totalh0.append(testh0.getPowerDemand(haushalte, day))
        totalg0.append(testg0.getPowerDemand(gewerbe, day))
        totalRlm.append(testRlm.getPowerDemand(industrie, day))

    totalh0 = np.asarray(totalh0).reshape((-1))
    totalg0 = np.asarray(totalg0).reshape((-1))
    totalRlm = np.asarray(totalRlm).reshape((-1))
    # #plt.plot(totalRlm)
    total = (totalRlm + totalh0 + totalg0)/1000000

    #plt.plot(totalRlm/ 10 ** 6)
    #plt.plot(totalg0 / 10 ** 6)
    #plt.plot(totalh0 / 10 ** 6)
    plt.plot(values)
    plt.plot(total)

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