import numpy as np
from components.basic_EnergySystem import energySystem as es
from pvlib.pvsystem import PVSystem
from pvlib.location import Location
from pvlib.modelchain import ModelChain
from pvlib.temperature import TEMPERATURE_MODEL_PARAMETERS
import pandas as pd


class pv_model(es):

    """ Haushalt mit Photovoltaik """
    def __init__(self,
                 lat=50.77, lon=6.09,                                                               # Längen- und Breitengrad
                 pdc0=0.24, azimuth=180, tilt=0,                                                    # Leistung [kW], Ausrichtung & Neigung der PV
                 t=np.arange(24), T=24, dt=1,                                                       # Metainfo Zeit
                 demQ=1000, demP=3000,                                                              # Jahresverbräuche
                 refSLP=np.asarray(np.load(open(r'./data/Ref_H0.array','rb')), np.float32),         # SLP Strom
                 refTemp=np.asarray(np.load(open(r'./data/Ref_Temp.array', 'rb')), np.float32),     # Referenztemperatur
                 factors=np.asarray(np.load(open(r'./data/Ref_Factors.array', 'rb')), np.float32),  # Stundenfaktoren
                 parameters=np.asarray([2.8, -37, 5.4, 0.17], np.float32)):                         # Gebäudeparameter
        super().__init__(t, T, dt, demQ, demP, refSLP, refTemp, factors, parameters)

        self.mySys = PVSystem(module_parameters=dict(pdc0=1000 * pdc0, gamma_pdc=-0.004),
                              inverter_parameters=dict(pdc0=1000 * pdc0),
                              surface_tilt=tilt, surface_azimuth=azimuth, albedo=0.25,
                              temperature_model_parameters=TEMPERATURE_MODEL_PARAMETERS['pvsyst']['insulated'],
                              losses_parameters=dict(availability=0, lid=0, shading=1, soiling=1))

        self.location = Location(lat, lon)

        self.mc = ModelChain(self.mySys, Location(lat, lon), aoi_model='physical', spectral_model='no_loss',
                             temperature_model='pvsyst', losses_model='pvwatts', ac_model='pvwatts')

    def build(self, data, ts, date):

        # Vorbereitung Wetterdaten für die Simulation
        weather = pd.DataFrame.from_dict(ts)
        weather['ghi'] = weather['dir'] + weather['dif']
        weather.columns = ['wind_speed', 'dni', 'dhi', 'temp_air', 'ghi']
        weather.index = pd.date_range(start=date, periods=len(weather), freq='60min')
        # Berechnung der Erzeugung aus PV auf Basis der Wetterdaten
        self.mc.run_model(weather)

        self.generation['solar'] = self.mc.ac.to_numpy() / 1000                                     # PV-Erzeugung pro Stunde in [kW]
        self.demand['power'] = self.getPowerDemand(data, date)                                      # Stromverbrauch pro Stunde in [kW]
        self.demand['heat'] = np.asarray(self.getHeatDemand(data, ts)).reshape((-1,))               # Wärmebedarf pro Stunde in [KW]

        # Verbleibene Last, die es zu minimieren gilt --> maximiere den Eigenverbrauch
        residuum = self.getPowerDemand(data, date) - self.generation['solar']
        for i in self.t:
            if residuum[i] < -0.7 * data['PV']['maxPower']:                                               # Einspeisung > 70 % der Nennleistung
                residuum[i] = -0.7 * data['PV']['maxPower']                                               # --> Kappung
                self.generation['solar'][i] = self.demand['power'][i] + 0.7 * data['PV']['maxPower']     # Erzeugung PV = 70 % der Nennleistung + Aktueller Bedarf

        # Strombedarf (Einspeisung) [kW]
        self.power = np.asarray(residuum, np.float).reshape((-1,))

if __name__ == "__main__":
        test = pv_model()