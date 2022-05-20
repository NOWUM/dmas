# third party modules
import numpy as np
from pvlib.pvsystem import PVSystem

# model modules
from systems.basic_system import EnergySystem as es
from demandlib.electric_profile import StandardLoadProfile


class PvBatModel(es):

    def __init__(self, T, demandP, batPower, maxPower, VMax, V0, eta, azimuth, tilt, *args, **kwargs):
        super().__init__(T)

        self.demand_system = StandardLoadProfile(type='household', demandP=demandP)
        self.pv_system = PVSystem(module_parameters=dict(pdc0=maxPower), surface_tilt=tilt, surface_azimuth=azimuth)
        self.battery_system = dict(v0=V0, v_max=VMax, efficiency=eta, maxPower=batPower, vt=np.zeros((self.T,)))

    def optimize(self):
        """
        :return: timer series in [kW]
        """
        # -> irradiance unit [W/m²]
        irradiance = self.pv_system.get_irradiance(solar_zenith=self.weather['zenith'],
                                                   solar_azimuth=self.weather['azimuth'],
                                                   dni=self.weather['dni'],
                                                   ghi=self.weather['ghi'],
                                                   dhi=self.weather['dhi'])
        # get generation in [kW]
        solar_power = (irradiance['poa_global'] / 10**3) * self.pv_system.arrays[0].module_parameters['pdc0']
        self.generation['solar'] = solar_power.to_numpy()
        # get demand in [kW]
        self.demand['power'] = self.demand_system.run_model(self.date)
        residual = self.demand['power'] - self.generation['solar']

        vt = self.battery_system['v0']

        grid_use, volume = [], []
        for r in residual:
            # ----- residual > 0 -----
            # case 1: storage is empty -> no usage
            if (r >= 0) & (vt == 0):
                grid_use.append(r)
                volume.append(0)
                continue
            # case 2: storage volume < residual -> use rest volume
            if (r >= 0) & (vt * self.battery_system['efficiency'] <= r):
                grid_use.append(r - vt * self.battery_system['efficiency'])
                vt = 0
                volume.append(0)
                continue
            # case 3: storage volume > residual -> use volume
            if (r >= 0) & (vt * self.battery_system['efficiency'] >= r):
                grid_use.append(0)
                vt -= r / self.battery_system['efficiency']
                volume.append(vt)
                continue
            # ----- residual < 0 -----
            # case 4: storage volume = vmax -> no usage
            if (r < 0) & (vt == self.battery_system['v_max']):
                grid_use.append(r)
                volume.append(self.battery_system['v_max'])
                continue
            # case 5: v_max - storage volume < residual -> use rest volume
            if (r < 0) & (vt - r * self.battery_system['efficiency'] <= self.battery_system['v_max']):
                grid_use.append(0)
                vt -= r * self.battery_system['efficiency']
                volume.append(vt)
                continue
            # case 6: v_max - storage volume > residual -> use volume
            if (r < 0) & (vt - r * self.battery_system['efficiency'] >= self.battery_system['v_max']):
                grid_use.append(r + (self.battery_system['v_max'] - vt) / self.battery_system['efficiency'])
                vt = self.battery_system['v_max']
                volume.append(vt)
                continue

        # set battery parameter
        self.battery_system['v0'] = vt
        self.battery_system['vt'] = volume

        # gird usage in [kW]
        self.power = np.asarray(grid_use, np.float).reshape((-1,))

        return self.power