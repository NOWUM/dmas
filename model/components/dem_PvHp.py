import numpy as np
from components.basic_EnergySystem import energySystem as es
from pvlib.pvsystem import PVSystem
from pvlib.location import Location
from pvlib.modelchain import ModelChain
from pvlib.temperature import TEMPERATURE_MODEL_PARAMETERS
import pandas as pd


class pvwp_model(es):
    """ Haushalt mit Photovoltaik und Wärmepumpe """
    def __init__(self,
                 lat=50.77, lon=6.09,
                 pdc0=240, azimuth=180, tilt=0,
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
        # Atkueller Speicherfüllstand in [l]
        vt = data['tank']['v0']

        weather = pd.DataFrame.from_dict(ts)
        weather['ghi'] = weather['dir'] + weather['dif']
        weather.columns = ['wind_speed', 'dni', 'dhi', 'temp_air', 'ghi']
        weather.index = pd.date_range(start=date, periods=len(weather), freq='60min')
        self.mc.run_model(weather)
        powerSolar = self.mc.ac

        # Verbleibene Last, die es zu minimieren gilt --> maximiere den Eigenverbrauch
        residuum = self.getPowerDemand(data, date) - powerSolar
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
