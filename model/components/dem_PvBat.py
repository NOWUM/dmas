import numpy as np
from components.basic_EnergySystem import energySystem as es

class pvbat_model(es):
    """ Haushalt mit Photovoltaik und Batteriespeicher """
    def __init__(self,
                 t=np.arange(24), T=24, dt=1,                                                       # Metainfo Zeit
                 demQ=1000, demP=3000,                                                              # Jahresverbräuche
                 refSLP=np.asarray(np.load(open(r'./data/Ref_H0.array','rb')), np.float32),         # SLP Strom
                 refTemp=np.asarray(np.load(open(r'./data/Ref_Temp.array', 'rb')), np.float32),     # Referenztemperatur
                 factors=np.asarray(np.load(open(r'./data/Ref_Factors.array', 'rb')), np.float32),  # Stundenfaktoren
                 parameters=np.asarray([2.8, -37, 5.4, 0.17], np.float32)):                         # Gebäudeparameter
        super().__init__(t, T, dt, demQ, demP, refSLP, refTemp, factors, parameters)

    def build(self, data, ts, date):
        # Atkueller Speicherfüllstand in [kWh]
        vt = data['Bat']['v0']

        # Verbleibene Last, die es zu minimieren gilt --> maximiere den Eigenverbrauch
        residuum = self.getPowerDemand(data, date) - self.getPowerSolar(data, ts)

        # für die verbleibende Leistung, plane die Speichernutzung
        grid = []
        storage = []
        for r in residuum:
            if (r >= 0) & (vt == 0):                                                            # keine Nutuzng möglich (leer)
                grid.append(r)
                storage.append(0)
                continue
            if (r >= 0) & (vt * self.dt * data['Bat']['eta'] <= r):                             # nutze Restkapazität
                grid.append(r - vt * self.dt * data['Bat']['eta'])                              # Rest --> Netz
                vt = 0
                storage.append(vt * self.dt * data['Bat']['eta'])
                continue
            if (r >= 0) & (vt * self.dt * data['Bat']['eta'] >= r):                             # entlade Speicher
                grid.append(0)
                vt -= r * self.dt / data['Bat']['eta']
                storage.append(r)
                continue
            if (r < 0) & (vt == data['Bat']['vmax']):                                           # keine Nutuzng möglich (voll)
                grid.append(r)
                storage.append(0)
                continue
            if (r < 0) & (vt - r * self.dt * data['Bat']['eta'] <= data['Bat']['vmax']):        # lade Speicher
                grid.append(0)
                vt -= r * self.dt * data['Bat']['eta']
                storage.append(r)
                continue
            if (r < 0) & (vt - r * self.dt * data['Bat']['eta'] >= data['Bat']['vmax']):        # lade bis voll
                grid.append(r + (data['Bat']['vmax'] - vt) * self.dt / data['Bat']['eta'])      # Rest --> Netz
                vt = data['Bat']['vmax']
                storage.append((data['Bat']['vmax'] - vt) * self.dt / data['Bat']['eta'])
                continue

        # Atkueller Speicherfüllstand in [kWh]
        data['Bat']['v0'] = vt
        # Speichernutzung [kW]
        self.powerStorage = np.asarray(storage, np.float).reshape((-1,))
        # Strombedarf [kW]
        self.powerDemand = np.asarray(grid, np.float).reshape((-1,))

if __name__ == "__main__":
    test = pvbat_model()
