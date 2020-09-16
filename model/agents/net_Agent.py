from agents.basic_Agent import agent as basicAgent
import os
os.chdir(os.path.dirname(os.path.dirname(__file__)))
import pandas as pd
import numpy as np
import pypsa
from plotly.colors import label_rgb, find_intermediate_color
import json
import plotly



    def __init__(self, date=pd.to_datetime('2019-02-01'), mongo='149.201.88.150', influx='149.201.88.150', market='149.201.88.150', dbName='MAS_2020'):
        super().__init__(date=date, plz=0, mongo=mongo, influx=influx, market=market, typ='NET', dbName=dbName)
class NetAgent(basicAgent):

    def __init__(self):

        self.network = pypsa.Network()

        # load grid data
        # --> check files and change to csv or pickle file
        buses = pd.read_excel(r'./data/Grid_Buses.xlsx', index_col=0)                   # read_excel --> read_csv/reac_pickle
        lines = pd.read_excel(r'./data/Grid_Lines.xlsx', index_col=0)
        transformers = pd.read_excel(r'./data/Grid_Trafos.xlsx', index_col=0)

        # build grid
        for i in range(len(buses)):
            try:
                values = buses.iloc[i, :]
                self.network.add("Bus", name=values['name'], v_nom=values['v_nom'], x=values['x'], y=values['y'])
            except:
                print('cant add Bus %s to network' % values['name'])

        for i in range(len(lines)):
            try:
                values = lines.iloc[i, :]
                self.network.add("Line", name=values['name'].split('_')[0] + '_' + str(i), bus0=values['bus0'], bus1=values['bus1'], x=values['x'], r=values['r'], s_nom=values['s_nom'])
            except Exception as e:
                print(e)
                print('cant add Line %s to network' % values['name'])

        for i in range(len(transformers)):
            try:
                values = transformers.iloc[i, :]
                self.network.add("Transformer", name=values['name'], bus0=values['bus0'], bus1=values['bus1'], s_nom=2000., x=22.9947/2000., r=0.3613/2000., model='t')
            except:
                print('cant add Transformator %s to network' % values['name'])

        for i in range(1, 100):
            if '%s_380' % i in buses.to_numpy() or '%s_220' % i in buses.to_numpy() and i not in [5, 11, 20, 43, 60, 62, 70, 80]:
                if '%s_380' % i in buses.to_numpy():
                    self.network.add("Load", 'Load_%s' % i, p_set=0, bus='%s_380' % i)
                    self.network.add("Generator", 'Gen_%s' % i, p_set=0, control='PV', bus='%s_380' % i)
                else:
                    self.network.add("Load", 'Load_%s' % i, p_set=0, bus='%s_220' % i)
                    self.network.add("Generator", 'Gen_%s' % i, p_set=0, control='PV', bus='%s_220' % i)

        print('Stop Building Grid')


    def calc_power_flow(self):

        # Step 1 get Data
        # self.connection['influxDB'].get_....(i,date)      # power for each area

        # Step 2 set data for grid calculation
        #index = 1
        #for row in np.asarray(total):
        #    if index not in [5, 11, 20, 43, 60, 62, 70, 80]:
        #        load = [val if val >= 0 else 0 for val in row]
        #        gen = [-1 * val if val < 0 else 0 for val in row]
        #        self.network.loads.loc['Load_%s' % index, 'p_set'] = load[hour]
        #        self.network.generators.loc['Gen_%s' % index, 'p_set'] = gen[hour]
        #    index += 1

        #self.network.pf(distribute_slack=True)

        # Step 3 build Dataframe to save results in influxDB/MongoDB
        # df = self.network.lines_t.p0.loc['now']
        # self.connections['influxDB'].save_data(df, 'Grid')                --> Variante 1
        # self.connections['influxDB'].influx.write_points(dataframe=df,    --> Variante 2
        # measurement='Grid', tag_columns=['names', 'order', 'typ'])

        pass

if __name__ == "__main__":

    # args = parse_args()
    agent = NetAgent(date='2018-01-01', plz=-1)
    # agent.connections['mongoDB'].login(agent.name, False)
    try:
        agent.run()
    except Exception as e:
        print(e)
    finally:
        agent.connections['influxDB'].influx.close()
        agent.connections['mongoDB'].mongo.close()
        if not agent.connections['connectionMQTT'].is_closed:
            agent.connections['connectionMQTT'].close()
        exit()
