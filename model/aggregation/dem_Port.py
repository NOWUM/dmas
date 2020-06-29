import numpy as np
from components.dem_PvBat import pvbat_model as pvBattery
from components.dem_PvSolo import pv_model as pvSolo
from components.dem_Consumer import h0_model, g0_model, rlm_model
from aggregation.basic_Port import port_model

class demPort(port_model):

    def __int__(self, T=24, dt=1, gurobi=False, date='2020-01-01', typ ='DEM'):
        super().__init__(T, dt, gurobi, date, typ)

    def addToPortfolio(self, name, energysystem):

        data = energysystem[name]

        # Photovoltaik mit Batteriespeicher
        if data['typ'] == 'PvBat':
            data.update(dict(model=pvBattery(lat=data['position'][0],                   # Längengrad
                                             lon=data['position'][1],                   # Breitengrad
                                             pdc0=data['PV']['maxPower'],               # Nennleistung
                                             azimuth=data['PV']['azimuth'],             # Ausrichtung (180° = Süd)
                                             tilt=data['PV']['tilt'],                   # Dachneigung
                                             t=self.t,                                  # Array mit Zeitschritten
                                             T=self.T,                                  # Anzahl an Zeitschritten
                                             dt=self.dt,                                # Zeitschrittlänge
                                             parameters=data['para'],                   # Gebäude Parameter (Wärmebedarf)
                                             demQ=data['demandQ'],                      # Wärmebedarf pro Jahr in kWh
                                             refTemp=self.Ref_Temperature,              # Ref. Temp für SLP-Wärme
                                             factors=self.Ref_factors,                  # Stundenfaktoren fur SLP-Wärme
                                             refSLP=self.Ref_H0)))                      # Ref. Profil für SLP-Strom
        # Photovoltaik
        elif data['typ'] == 'Pv':
            data.update(dict(model=pvSolo(lat=data['position'][0],                      # Längengrad
                                          lon=data['position'][1],                      # Breitengrad
                                          pdc0=data['PV']['maxPower'],                  # Nennleistung
                                          azimuth=data['PV']['azimuth'],                # Ausrichtung (180° = Süd)
                                          tilt=data['PV']['tilt'],                      # Dachneigung
                                          t=self.t,                                     # Array mit Zeitschritten
                                          T=self.T,                                     # Anzahl an Zeitschritten
                                          dt=self.dt,                                   # Zeitschrittlänge
                                          parameters=data['para'],                      # Gebäude Parameter (Wärmebedarf)
                                          demQ=data['demandQ'],                         # Wärmebedarf pro Jahr in kWh
                                          refTemp=self.Ref_Temperature,                 # Ref. Temp für SLP-Wärme
                                          factors=self.Ref_factors,                     # Stundenfaktoren fur SLP-Wärme
                                          refSLP=self.Ref_H0)))                         # Ref. Profil für SLP-Strom
        # Standard Haushalt
        elif data['typ'] == 'H0':
            data.update(dict(model=h0_model(t=self.t,                                   # Array mit Zeitschritten
                                            T=self.T,                                   # Anzahl an Zeitschritten
                                            dt=self.dt,                                 # Zeitschrittlänge
                                            refSLP=self.Ref_H0)))                       # Ref. Profil für SLP-Strom H0
        # Standard Gewerbe
        elif data['typ'] == 'G0':
            data.update(dict(model=g0_model(t=self.t,                                   # Array mit Zeitschritten
                                            T=self.T,                                   # Anzahl an Zeitschritten
                                            dt=self.dt,                                 # Zeitschrittlänge
                                            refSLP=self.Ref_G0)))                       # Ref. Profil für SLP-Strom G0
        # Standard Industrie
        elif data['typ'] == 'RLM':
            data.update(dict(model=rlm_model(t=self.t,                                  # Array mit Zeitschritten
                                             T=self.T,                                  # Anzahl an Zeitschritten
                                             dt=self.dt,                                # Zeitschrittlänge
                                             refSLP=self.Ref_Rlm)))                     # Ref. Profil für SLP-Strom RLM

        self.energySystems.update(energysystem)

    def buildModel(self, response=[]):
        for _, data in self.energySystems.items():
            data['model'].build(data, self.weather, self.date)


    def optimize(self):

        power = np.zeros_like(self.t)               # Leistungsbilanz des Gebietes

        try:
            # Leitungsbilanz
            power = np.asarray([(value['model'].power * (value['num'] - value['EEG'])) if 'Pv' in key else value['model'].power
                                for key, value in self.energySystems.items()], np.float)
            power = np.sum(power, axis=0)
            # PV-Erzeugung
            solar = np.asarray([(value['model'].generation['solar'] * (value['num'] - value['EEG'])) if 'Pv' in key else
                                value['model'].generation['solar'] for key, value in self.energySystems.items()], np.float)
            self.generation['solar'] = np.sum(solar, axis=0)
            # Strombedarf
            demandP = np.asarray([(value['model'].demand['power'] * (value['num'] - value['EEG'])) if 'Pv' in key else
                                  value['model'].demand['power'] for key, value in self.energySystems.items()], np.float)
            self.demand['power'] = np.sum(demandP, axis=0)
            # Wärmebedarf
            demandQ = np.asarray([(value['model'].demand['heat'] * (value['num'] - value['EEG'])) if 'Pv' in key else
                                  value['model'].demand['heat'] for key, value in self.energySystems.items()], np.float)
            self.demand['heat'] = np.sum(demandQ, axis=0)

        except Exception as e:
            print(e)
        self.power = power                              # Leistungsbilanz des Gebietes
        return power

    def fixPlaning(self):
        power = np.zeros_like(self.t)
        try:
            err = np.random.normal(loc=0.013, scale=0.037, size=self.T)
            power = self.power / (1 - err)
        except Exception as e:
            print(e)
        self.power = power
        return power

if __name__ == "__main__":
    test = demPort()