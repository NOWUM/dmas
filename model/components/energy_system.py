import numpy as np


class EnergySystem:

    def __init__(self, t=np.arange(24), T=24, dt=1):
        self.t, self.T, self.dt = t, T, dt

        self.demand = dict(power=np.zeros_like(self.t),     # elec. power
                           heat=np.zeros_like(self.t))      # heat demand

        self.generation = {}

    def set_parameter(self):
        pass

    def optimize(self):
        pass
