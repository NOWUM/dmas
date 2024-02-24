# third party modules
import numpy as np
import pandas as pd
from pvlib.pvsystem import PVSystem
# model modules
from systems.basic_system import EnergySystem


def get_solar_generation(generation_system: PVSystem, weather: pd.DataFrame) -> np.array:
    ir = generation_system.get_irradiance(solar_zenith=weather['zenith'], solar_azimuth=weather['azimuth'],
                                          dni=weather['dni'], ghi=weather['ghi'], dhi=weather['dhi'])
    power = ir['poa_global'] * generation_system.arrays[0].module_parameters['pdc0'] / 1e3
    return np.asarray(power).flatten()


class Prosumer(EnergySystem):

    def __init__(self, T: int, maxPower: float, demandP: float = 0, azimuth: int = 180, tilt: int = 35,
                 storage: bool = False, *args, **kwargs):
        super().__init__(T, demandP=demandP, maxPower=maxPower, fuel_type='solar', demand_type='household')

        self.generation_system = PVSystem(module_parameters=dict(pdc0=maxPower), surface_tilt=tilt,
                                          surface_azimuth=azimuth)
        self.storage_system = None
        if storage:
            self.storage_system = dict(v0=kwargs['V0'], v_max=kwargs['VMax'], vt=np.zeros(self.T),
                                       efficiency=kwargs['eta'], maxPower=kwargs['powerPower'])

    def _get_generation(self) -> np.array:
        if self.fuel_type == 'solar':
            power = get_solar_generation(self.generation_system, weather=self.weather)
        else:
            power = np.zeros(self.T)
        return power

    def _get_demand(self, d_type: str = 'power') -> np.array:
        power = self.demand_generator.run_model(self.date)
        return np.asarray(power).flatten()

    def _use_storage(self) -> np.array:

        residual = self.demand['power'] - self.generation['solar']

        vt = self.storage_system['v0']

        grid_use, volume = [], []
        for r in residual:
            # ----- residual > 0 -----
            # case 1: storage is empty -> no usage
            if (r >= 0) & (vt == 0):
                grid_use.append(r)
                volume.append(0)
                continue
            # case 2: storage volume < residual -> use rest volume
            if (r >= 0) & (vt * self.storage_system['efficiency'] <= r):
                grid_use.append(r - vt * self.storage_system['efficiency'])
                vt = 0
                volume.append(0)
                continue
            # case 3: storage volume > residual -> use volume
            if (r >= 0) & (vt * self.storage_system['efficiency'] >= r):
                grid_use.append(0)
                vt -= r / self.storage_system['efficiency']
                volume.append(vt)
                continue
            # ----- residual < 0 -----
            # case 4: storage volume = vmax -> no usage
            if (r < 0) & (vt == self.storage_system['v_max']):
                grid_use.append(r)
                volume.append(self.storage_system['v_max'])
                continue
            # case 5: v_max - storage volume < residual -> use rest volume
            if (r < 0) & (vt - r * self.storage_system['efficiency'] <= self.storage_system['v_max']):
                grid_use.append(0)
                vt -= r * self.storage_system['efficiency']
                volume.append(vt)
                continue
            # case 6: v_max - storage volume > residual -> use volume
            if (r < 0) & (vt - r * self.storage_system['efficiency'] >= self.storage_system['v_max']):
                grid_use.append(r + (self.storage_system['v_max'] - vt) / self.storage_system['efficiency'])
                vt = self.storage_system['v_max']
                volume.append(vt)
                continue

        # set battery parameter
        self.storage_system['v0'] = vt
        self.storage_system['vt'] = np.asarray(volume)
        self.volume = self.storage_system['vt']

        # grid usage in [kW]
        self.power = np.asarray(grid_use, float).reshape((-1,))

    def optimize(self, date: pd.Timestamp = None, weather: pd.DataFrame = None, prices: pd.DataFrame = None,
                 steps=None) -> np.array:
        """
        :return: timer series in [kW]
        """
        self._reset_data()
        self._set_parameter(date=date, weather=weather, prices=prices)

        self.demand['power'] = self._get_demand()
        self.generation['solar'] = self._get_generation()
        self._set_total_generation()
        if self.storage_system is None:
            self.power = self.demand['power'] - self.generation['solar']
        else:
            self._use_storage()

        return self.power

