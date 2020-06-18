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
                 lat=50.77, lon=6.09,
                 pdc0=240, azimut=180, tilt=0,
                 t=np.arange(24), T=24, dt=1,                                                       # Metainfo Zeit
                 demQ=1000, demP=3000,                                                              # Jahresverbräuche
                 refSLP=np.asarray(np.load(open(r'./data/Ref_H0.array','rb')), np.float32),         # SLP Strom
                 refTemp=np.asarray(np.load(open(r'./data/Ref_Temp.array', 'rb')), np.float32),     # Referenztemperatur
                 factors=np.asarray(np.load(open(r'./data/Ref_Factors.array', 'rb')), np.float32),  # Stundenfaktoren
                 parameters=np.asarray([2.8, -37, 5.4, 0.17], np.float32)):                         # Gebäudeparameter
        super().__init__(t, T, dt, demQ, demP, refSLP, refTemp, factors, parameters)

        temperature_model_parameters = TEMPERATURE_MODEL_PARAMETERS['sapm']['open_rack_glass_glass']
        mySys = PVSystem(module_parameters=dict(pdc0=pdc0, gamma_pdc=-0.004), inverter_parameters=dict(pdc0=pdc0),
                         surface_tilt=tilt, surface_azimuth=azimut, albedo=0.25,
                         temperature_model_parameters=temperature_model_parameters)
        self.mc = ModelChain(mySys, Location(lat, lon), aoi_model='physical', spectral_model='no_loss')

    def build(self, data, ts, date):

        weather = pd.DataFrame.from_dict(ts)
        weather['ghi'] = weather['dir'] + weather['dif']
        weather.columns = ['wind_speed', 'dni', 'dhi', 'temp_air', 'ghi']
        weather.index = pd.date_range(start=date, periods=len(weather), freq='60min')
        self.mc.run_model(weather)
        powerSolar = self.mc.ac

        # Verbleibene Last, die es zu minimieren gilt --> maximiere den Eigenverbrauch
        residuum = self.getPowerDemand(data, date) - powerSolar
        # Strombedarf (Einspeisung) [kW]
        self.powerDemand = np.asarray(residuum, np.float).reshape((-1,))

if __name__ == "__main__":
        test = pv_model()