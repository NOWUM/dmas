import numpy as np
from components.basic_EnergySystem import energySystem as es

class pvwp_model(es):
    """ Haushalt mit Photovoltaik und Wärmepumpe """
    def __init__(self,
                 t=np.arange(24), T=24, dt=1,                                                       # Metainfo Zeit
                 demQ=1000, demP=3000,                                                              # Jahresverbräuche
                 refSLP=np.asarray(np.load(open(r'./data/Ref_H0.array','rb')), np.float32),         # SLP Strom
                 refTemp=np.asarray(np.load(open(r'./data/Ref_Temp.array', 'rb')), np.float32),     # Referenztemperatur
                 factors=np.asarray(np.load(open(r'./data/Ref_Factors.array', 'rb')), np.float32),  # Stundenfaktoren
                 parameters=np.asarray([2.8, -37, 5.4, 0.17], np.float32)):                         # Gebäudeparameter
        super().__init__(t, T, dt, demQ, demP, refSLP, refTemp, factors, parameters)

    def build(self, data, ts, date):
        # Atkueller Speicherfüllstand in [l]
        vt = data['tank']['v0']
        # Verbleibene Last, die es zu minimieren gilt --> maximiere den Eigenverbrauch
        residuum = self.getPowerDemand(data, date) - self.getPowerSolar(data, ts)
        # Wärmebedarf
        heatDemand = self.getHeatDemand(data, ts)

        # für die verbleibende Leistung, plane die Speichernutzung
        grid = []
        for i in range(len(residuum)):
            if (residuum[i] >= 0) & (vt == 0) | (heatDemand[i] > vt):                                # keine Nutuzng möglich (leer)
                q_wp = heatDemand[i]
                grid.append(residuum[i] + q_wp/data['HP']['cop'])
                continue
            if (residuum[i] >= 0) & (heatDemand[i] <= vt):                                          # Nutze Wärmespeicher
                vt -= heatDemand[i]
                grid.append(residuum[i])
                continue
            if (residuum[i] <= 0):                                                                  # Eigenerzeugung
                # Strombedarf resultierend aus Wärmebedarf
                powHp = heatDemand[i]/data['HP']['cop']
                if (powHp < residuum[i]) & (vt - (residuum[i] + powHp)*data['HP']['cop'] < data['tank']['vmax']):
                    vt -= (residuum[i] + powHp)*data['HP']['cop']                                   # entlade Speicher
                    grid.append(0)
                    continue
                if (powHp < residuum[i]) & (vt - (residuum[i] + powHp)*data['HP']['cop'] >= data['tank']['vmax']):
                    grid.append((residuum[i] + powHp)-(data['tank']['vmax']-vt)/data['HP']['cop'])  # Lade Speicher
                    vt = data['tank']['vmax']
                    continue
                if (powHp > residuum[i]):                                                           # --> Rest Netz
                    grid.append(powHp+residuum[i])
                    continue
        # Atkueller Speicherfüllstand in [l]
        data['tank']['v0'] = vt
        self.powerDemand = np.asarray(grid, np.float).reshape((-1,))
        self.heatDemand = np.asarray(heatDemand, np.float).reshape((-1,))


if __name__ == "__main__":
    test = pvwp_model()
