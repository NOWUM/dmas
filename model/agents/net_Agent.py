import pandas as pd
import numpy as np
import pypsa
from agents.basic_Agent import agent as basicAgent
from plotly.colors import label_rgb, find_intermediate_color
import json
import plotly


class netAgent(basicAgent):

    def __init__(self, date=pd.to_datetime('2019-02-01'), mongo='149.201.88.150', influx='149.201.88.150', market='149.201.88.150', dbName='MAS_2020'):
        super().__init__(date=date, plz=0, mongo=mongo, influx=influx, market=market, typ='NET', dbName=dbName)

        print('Start Building Grid')
        self.network = pypsa.Network()
        self.buses = pd.read_csv(r'./data/Grid_Bus.csv', sep=';', decimal=',').to_numpy()
        self.lines = pd.read_csv(r'./data/Grid_Line.csv', sep=';', decimal=',').to_numpy()
        self.transformers = pd.read_csv(r'./data/Grid_Transformer.csv', sep=';', decimal=',').to_numpy()

        for bus in self.buses:
            try:
                busName = str(bus[0])
                busVoltage = bus[1]
                busLat = bus[2]
                busLon = bus[3]
                self.network.add("Bus", name=busName, v_nom=busVoltage, x=busLon, y=busLat)
            except:
                print('cant add Bus %s to network' % busName)

        for line in self.lines:
            try:
                lineName = line[1] + "_" + line[2]
                lineBusFrom = line[1]
                lineBusTo = line[2]
                lineX = line[5]
                lineR = line[6]
                lineS_nom = line[3]
                self.network.add("Line", lineName, bus0=lineBusFrom, bus1=lineBusTo, x=lineX, r=lineR, s_nom=lineS_nom)
            except:
                print('cant add Line %s to network' % lineName)

        for trafo in self.transformers:
            try:
                name = trafo[0]
                busFrom = trafo[1]
                busTo = trafo[2]
                s_nom = trafo[8]
                self.network.add("Transformer", name=name, bus0=busFrom, bus1=busTo, s_nom=1000., x=22.9947/s_nom, r=0.3613/s_nom, model='t')
            except:
                print('cant add Transformator %s to network' % name)

        for i in range(1, 100):
            if '%s_380' % i in self.buses or '%s_220' % i in self.buses and i not in [5, 11, 43, 62, 80]:
                if '%s_380' % i in self.buses:
                    self.network.add("Load", 'Load_%s' % i, p_set=0, bus='%s_380' % i)
                    self.network.add("Generator", 'Gen_%s' % i, p_set=0, control='PV', bus='%s_380' % i)
                else:
                    self.network.add("Load", 'Load_%s' % i, p_set=0, bus='%s_220' % i)
                    self.network.add("Generator", 'Gen_%s' % i, p_set=0, control='PV', bus='%s_220' % i)

        print('Stop Building Grid')

    def getPowerFlow(self):
        total = [self.ConnectionInflux.getPowerArea(date=self.date, area=i) for i in range(1, 100)]
        # [20 (22) 60 (61) 70 (71) 80 (81)]
        index = 1
        for i in range(24):
            for row in np.asarray(total):
                if index not in [5, 11, 43, 62, 80]:
                    load = [val if val >= 0 else 0 for val in row]
                    gen = [-1 * val if val < 0 else 0 for val in row]
                    self.network.loads.loc['Load_%s' % index, 'p_set'] = load[i]
                    self.network.generators.loc['Gen_%s' % index, 'p_set'] = gen[i]
                index += 1

            self.network.pf(distribute_slack=True)

if __name__ == "__main__":

    agent = netAgent()
    agent.getPowerFlow()