# third party modules
import numpy as np
import threading, queue
import multiprocessing as mp

# model modules
from systems.demand_pv_bat import PvBatModel
from systems.demand_pv import HouseholdPvModel
from systems.demand import HouseholdModel, BusinessModel, IndustryModel
from aggregation.portfolio import PortfolioModel
import pandas as pd

import logging
import time
log = logging.getLogger()
log.setLevel('INFO')

def do_work(in_queue, out_queue):
    i = 0
    while in_queue.qsize() > 0:
        item = in_queue.get()
        item['model'].optimize()
        item['model'].generation['solar'][0] = 1
        out_queue.put(item)
        #log.info(f'New {i}, {in_queue.qsize()}')
        #i +=1
    log.info('finished')

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

        self.energy_systems.append(energy_system)

    def build_model(self, response=None):
        t = time.time()
        log.info('start time')
        self.weather['ghi'] = self.weather['dir'] + self.weather['dif']
        self.weather.columns = ['wind_speed', 'dni', 'dhi', 'temp_air', 'ghi']
        self.weather.index = pd.date_range(start=self.date, periods=len(self.weather), freq='60min')

        for data in self.energy_systems:
            data['model'].set_parameter(weather=self.weather, date=self.date)
        log.info(time.time() - t)


    def f(self, item):
        item['model'].optimize()
        item['model'].generation['solar'][0] = 1
        return item

    def optimize(self):
        t = time.time()
        pool = False
        if pool:
            with mp.Pool(6) as p:
                v = p.map(self.f, self.energy_systems)
            self.energy_systems = v
        else:
            processes = []
            in_q = mp.Queue()
            out_q = mp.Queue()
            for en in self.energy_systems:
                in_q.put(en)

            for i in range(6):
                p = mp.Process(target=do_work, args=(in_q, out_q))
                p.daemon = True
                p.name = 'worker' + str(i)
                processes.append(p)
                p.start()

            while not in_q.empty():
                    time.sleep(0.2)
            for proc in processes:
                proc.join(0)
            log.info(in_q.empty())
            es = []
            while not out_q.empty():
                es.append(out_q.get())
            self.energy_systems = es

        log.info(time.time() - t)
        t = time.time()
        power, solar, demand = [], [], []
        for value in self.energy_systems:
            if 'solar' in value['type'] or 'battery' in value['type']:
                solar.append(value['model'].generation['solar'])

            power.append(value['model'].power)
            demand.append(value['model'].demand['power'])

        self.generation['solar'] = np.sum(np.asarray(solar, np.float), axis=0)
        self.demand['power'] = np.sum(np.asarray(demand, np.float), axis=0)
        self.generation['total'] = self.generation['solar']

        self.power = self.generation['total'] - self.demand['power']
        log.info(time.time() - t)
        t = time.time()
        return self.power


if __name__ == "__main__":

    portfolio = DemandPortfolio()