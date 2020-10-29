# third party modules
import os
import numpy as np
import pandas as pd

# model modules
from components.energy_system import EnergySystem as es
from apps.slpP import slpGen as slpP
os.chdir(os.path.dirname(os.path.dirname(__file__)))


class H0Model(es):

    def __init__(self, t=np.arange(24), T=24, dt=1, e_el=3000):
        super().__init__(t, T, dt)
        # initialize standard h0 consumer attributes
        self.e_el = e_el
        self.slpP = slpP(typ=0, refSLP=np.asarray(np.load(open(r'./data/Ref_H0.array','rb')), np.float32))

    def optimize(self):
        # adjustment Due to the overestimated simultaneity in the SLP
        power = self.e_el * 0.2
        base = self.e_el * 0.8

        demand = self.slpP.get_profile(self.date.dayofyear, self.date.dayofweek, power).reshape((96, 1))
        if self.T == 24:
            demand = np.asarray([np.mean(demand[i:i+3]) for i in range(0,96,4)], np.float).reshape((-1,)) + base / 8760

        self.demand['power'] = demand
        self.power = np.asarray(demand, np.float).reshape((-1,))

        return self.power


class G0Model(es):

    def __init__(self, t=np.arange(24), T=24, dt=1, e_el=3000):
        super().__init__(t, T, dt)
        # initialize standard h0 consumer attributes
        self.date = pd.to_datetime('2018-01-01')
        self.e_el = e_el
        self.slpP = slpP(typ=1, refSLP=np.asarray(np.load(open(r'./data/Ref_G0.array', 'rb')), np.float32))

    def optimize(self):
        # adjustment Due to the overestimated simultaneity in the SLP
        power = self.e_el * 0.2
        base = self.e_el * 0.8

        demand = self.slpP.get_profile(self.date.dayofyear, self.date.dayofweek, power).reshape((96, 1))
        if self.T == 24:
            demand = np.asarray([np.mean(demand[i:i + 3]) for i in range(0, 96, 4)], np.float).reshape(
                (-1,)) + base / 8760

        self.demand['power'] = demand
        self.power = np.asarray(demand, np.float).reshape((-1,))

        return self.power


class RlmModel(es):

    def __init__(self, t=np.arange(24), T=24, dt=1, e_el=3000):
        super().__init__(t, T, dt)
        # initialize standard h0 consumer attributes
        self.date = pd.to_datetime('2018-01-01')
        self.e_el = e_el
        self.slpP = slpP(typ=1, refSLP=np.asarray(np.load(open(r'./data/Ref_RLM.array', 'rb')), np.float32))

    def optimize(self):
        # adjustment Due to the overestimated simultaneity in the SLP
        power = self.e_el * 0.2
        base = self.e_el * 0.8

        demand = self.slpP.get_profile(self.date.dayofyear, self.date.dayofweek, power).reshape((96, 1))
        if self.T == 24:
            demand = np.asarray([np.mean(demand[i:i + 3]) for i in range(0, 96, 4)], np.float).reshape(
                (-1,)) + base / 8760

        self.demand['power'] = demand
        self.power = np.asarray(demand, np.float).reshape((-1,))

        return self.power


if __name__ == "__main__":

    h0_consumer = H0Model()
    h0_power = h0_consumer.optimize()
    g0_consumer = G0Model()
    g0_power = g0_consumer.optimize()
    rlm_consumer = RlmModel()
    rlm_power = rlm_consumer.optimize()
