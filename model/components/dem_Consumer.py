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
        power = data['demandP'] * 0.3           # Variabler Anteil
        base = data['demandP'] * 0.7            # Konstanter Anteil
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
        super().__init__(t, T, dt, demQ, demP, refSLP, refTemp, factors, parameters)

    def getPowerDemand(self, data, date):
        power = data['demandP'] * 0.3           # Variabler Anteil
        base = data['demandP'] * 0.7            # Konstanter Anteil
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
        super().__init__(t, T, dt, demQ, demP, refSLP, refTemp, factors, parameters)

    def getPowerDemand(self, data, date):
        data['demandP'] *= 0.775              # Korrektzur um Bahnnetz und Eigenerzeugung der Industrie
        power = data['demandP'] * 0.9         # Variabler Anteil
        base = data['demandP'] * 0.1          # Konstanter Anteil
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


    mongoDB = mongoInterface(database='MAS_XXXX')


    df = pd.read_excel(r'C:\Users\Administrator\Desktop\dmas\model\data\load.xlsx', index_col=0)
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

    for i in range(1,100):
        try:
            demand = mongoDB.getDemand(i)
            haushalte += demand['h0']
            gewerbe += demand['g0']
            industrie += demand['rlm']
        except:
            print('keine Lastdaten')

    tmp = industrie
    industrie = dict(demandP=177494*10**6)
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
    #total1 = total[1:2126]
    #total2 = total[2127:7176]
    #total3 = total[7201:]
    #total4 = np.concatenate((total1, total2, total3))
    #
    plt.plot(total)
    plt.plot(values)
    #plt.plot(values)
    #plt.plot(total)
    #plt.plot(values[:-27]-total4)
    # print('Mittelwert %s' % np.mean(total))
    # print('Minimum %s' % np.min(total))
    # print('Maximum %s' % np.max(total))

    # sommer=np.asarray(np.load(open(r'./data/Time_Summer.array','rb')), np.int64)
    # winter = np.asarray(np.load(open(r'./data/Time_Winter.array', 'rb')), np.int64)
    # x = values-total
    # plt.plot(x)
    # rlm = pd.DataFrame(x)
    # print(np.sum(rlm))
    # #
    # rlm = rlm/np.sum(rlm)*1000000
    # rlm.index = pd.date_range(start='2019-01-01', periods=8760, freq='h')
    # #rlm = rlm.iloc[:-21]
    # typDays = []
    #
    # result = rlm.loc[[i.dayofyear in winter for i in rlm.index]]
    # so = result.loc[[i.dayofweek == 6 for i in result.index]]
    # so = so.groupby(so.index.hour).mean().to_numpy().reshape(-1)
    # sa = result.loc[[i.dayofweek == 5 for i in result.index]]
    # sa = sa.groupby(sa.index.hour).mean().to_numpy().reshape(-1)
    # wt = result.loc[[i.dayofweek in [0, 1, 2, 3, 4] for i in result.index]]
    # wt = wt.groupby(wt.index.hour).mean().to_numpy().reshape(-1)
    #
    # typDays.append([element for element in sa for _ in range(4)])
    # typDays.append([element for element in so for _ in range(4)])
    # typDays.append([element for element in wt for _ in range(4)])
    #
    # result = rlm.loc[[i.dayofyear in sommer for i in rlm.index]]
    # so = result.loc[[i.dayofweek == 6 for i in result.index]]
    # so = so.groupby(so.index.hour).mean().to_numpy().reshape(-1)
    # sa = result.loc[[i.dayofweek == 5 for i in result.index]]
    # sa = sa.groupby(sa.index.hour).mean().to_numpy().reshape(-1)
    # wt = result.loc[[i.dayofweek in [0, 1, 2, 3, 4] for i in result.index]]
    # wt = wt.groupby(wt.index.hour).mean().to_numpy().reshape(-1)
    #
    # typDays.append([element for element in sa for _ in range(4)])
    # typDays.append([element for element in so for _ in range(4)])
    # typDays.append([element for element in wt for _ in range(4)])
    #
    # result = rlm.loc[[(i.dayofyear not in sommer) or i.dayofyear not in winter for i in rlm.index]]
    # so = result.loc[[i.dayofweek == 6 for i in result.index]]
    # so = so.groupby(so.index.hour).mean().to_numpy().reshape(-1)
    # sa = result.loc[[i.dayofweek == 5 for i in result.index]]
    # sa = sa.groupby(sa.index.hour).mean().to_numpy().reshape(-1)
    # wt = result.loc[[i.dayofweek in [0, 1, 2, 3, 4] for i in result.index]]
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
    # np.save(outfile,x)
    #
    # outfile.close()