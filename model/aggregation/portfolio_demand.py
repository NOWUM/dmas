# third party modules
import numpy as np
import threading, queue

# model modules
from systems.demand_pv_bat import PvBatModel
from systems.demand_pv import HouseholdPvModel
from systems.demand import HouseholdModel, BusinessModel, IndustryModel
from aggregation.portfolio import PortfolioModel


class DemandPortfolio(PortfolioModel):

    def __int__(self, T=24, date='2020-01-01'):
        super().__init__(T, date)

    def add_energy_system(self, energy_system):

        if energy_system['type'] == 'battery':
            energy_system.update(dict(model=PvBatModel(T=self.T, **energy_system)))
            self.capacities['solar'] += energy_system['maxPower']
        elif energy_system['type'] == 'solar':
            energy_system.update(dict(model=HouseholdPvModel(T=self.T, **energy_system)))
            self.capacities['solar'] += energy_system['maxPower']
        elif energy_system['type'] == 'household':
            energy_system.update(dict(model=HouseholdModel(T=self.T, **energy_system)))
        elif energy_system['type'] == 'business':
            energy_system.update(dict(model=BusinessModel(T=self.T, **energy_system)))
        elif energy_system['type'] == 'industry':
            energy_system.update(dict(model=IndustryModel(T=self.T, **energy_system)))

        self.energy_systems.update({energy_system['unitID']: energy_system})

    def build_model(self, response=None):
        for _, data in self.energy_systems.items():
            data['model'].set_parameter(weather=self.weather, date=self.date)

    def optimize(self):

        q = queue.Queue()

        def worker():
            while True:
                item = q.get()
                item.optimize()
                q.task_done()

        for _, data in self.energy_systems.items():
            q.put(data['model'])
        for i in range(20):
            threading.Thread(target=worker, daemon=True).start()

        q.join()

        power, solar, demand = [], [], []
        for _, value in self.energy_systems.items():
            if 'solar' in value['type'] or 'battery' in value['type']:
                solar.append(value['model'].generation['solar'])

            power.append(value['model'].power)
            demand.append(value['model'].demand['power'])

        self.generation['solar'] = np.sum(np.asarray(solar, np.float), axis=0)
        self.demand['power'] = np.sum(np.asarray(demand, np.float), axis=0)
        self.generation['total'] = self.generation['solar']

        self.power = self.generation['total'] - self.demand['power']

        return self.power


if __name__ == "__main__":

    portfolio = DemandPortfolio()

