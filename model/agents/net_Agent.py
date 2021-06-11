# third party modules
from sys import exit
import os
import argparse
import time as tme
import pandas as pd
import numpy as np
import pypsa

# model modules
from agents.basic_Agent import agent as basicAgent
os.chdir(os.path.dirname(os.path.dirname(__file__)))

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--plz', type=int, required=False, default=24, help='PLZ-Agent')
    return parser.parse_args()

class NetAgent(basicAgent):

    def __init__(self, date, plz):
        super().__init__(date=date, plz=plz, typ='NET')
        self.logger.info('starting the agent')
        # create pypsa instance for powerflow
        self.network = pypsa.Network()
        start_time = tme.time()

        # load raw grid data and build grid structure
        buses = pd.read_csv(r'./data/Grid_Buses.csv', sep=';', decimal=',', index_col=0)
        lines = pd.read_csv(r'./data/Grid_Lines.csv', sep=';', decimal=',', index_col=0)
        transformers = pd.read_csv(r'./data/Grid_Trafos.csv', sep=';', decimal=',', index_col=0)

        self.bus_names = buses['name'].to_numpy()
        self.area_num = pd.read_csv(r'./data/Ref_GeoInfo.csv',sep=';', decimal=',', index_col=0)
        self.area_num = self.area_num['PLZ'].to_numpy(dtype=int)

        # add nodes to network
        for i in range(len(buses)):
            values = buses.iloc[i, :]
            self.network.add("Bus", name=values['name'], v_nom=values['v_nom'], x=values['x'], y=values['y'])
        self.logger.info('nodes added')

        # add lines to network
        for i in range(len(lines)):
            values = lines.iloc[i, :]
            self.network.add("Line", name=values['name'].split('_')[0] + '_' + str(i), bus0=values['bus0'],
                             bus1=values['bus1'], x=values['x'], r=values['r'], s_nom=values['s_nom'])
        self.logger.info('lines added')

        # add transformers to network
        for i in range(len(transformers)):
            values = transformers.iloc[i, :]
            self.network.add("Transformer", name=values['name'],
                             bus0=values['bus0'], bus1=values['bus1'],
                             s_nom=2000., x=22.9947/2000., r=0.3613/2000., model='t')
        self.logger.info('transformers added')

        # add load and generation at each bus
        for bus in buses['name'].to_numpy():
            self.network.add("Load", 'Load_%s' % bus, p_set=0, bus=bus)
            self.network.add("Generator", 'Gen_%s' % bus, p_set=0, control='PV', bus=bus)
        self.logger.info('load and generation added')

        self.logger.info('setup of the agent completed in %s' % (tme.time() - start_time))

    def calc_power_flow(self): # Hier findet die Netzberechnung statt
        """power flow calculation 380kV and 220kV grid"""
        self.logger.info('Power flow calculation started')

        # Step 1: Get power data from database
        # -------------------------------------------------------------------------------------------------------------
        power_total = np.asarray([self.connections['influxDB'].get_power_area(date=self.date, area=str(i))
                                  for i in self.area_num], dtype=float)

        # Step 2: Set data for grid calculation and get power flow
        # -------------------------------------------------------------------------------------------------------------
        time = self.date

        for hour in range(24):
            values = power_total[:, hour]
            index = 0
            for value in values:
                area_220 = str(self.area_num[index]) + '_220'       # get node name for 220 kV
                area_380 = str(self.area_num[index]) + '_380'       # get node name for 380 kV

                l, g = 0, 0                                         # build load and generation
                if value > 0:
                    l = value                                       # set load value if positive    (bid > ask)
                elif value < 0:
                    g = -value                                      # set gen value if negative     (bid < ask)

                # check if both nodes are in network
                if area_220 in self.bus_names and area_380 in self.bus_names:
                    # loads on 220 kV and 380 kV
                    self.network.loads.loc['Load_%s' % area_220, 'p_set'] = 0.7 * l         # set load value 220 kV
                    self.network.loads.loc['Load_%s' % area_380, 'p_set'] = 0.3 * l         # set load value 380 kV
                    # generation on 220 kV and 380 kV
                    self.network.generators.loc['Gen_%s' % area_220, 'p_set'] = 0.1 * g     # set gen. value 220 kV
                    self.network.generators.loc['Gen_%s' % area_380, 'p_set'] = 0.9 * g     # set gen. value 380 kV
                # check if only 380 kV node is network
                elif area_380 in self.bus_names:
                    self.network.loads.loc['Load_%s' % area_380, 'p_set'] = l               # set load value 380 kV
                    self.network.generators.loc['Gen_%s' % area_380, 'p_set'] = g           # set gen. value 380 kV
                # check if only 220 kV node is network
                elif area_220 in self.bus_names:
                    self.network.loads.loc['Load_%s' % area_220, 'p_set'] = l               # set load value 220 kV
                    self.network.generators.loc['Gen_%s' % area_220, 'p_set'] = g           # set gen. value 220 kV

                index += 1

            self.network.pf(distribute_slack=True)                                          # calculate power flow

            # Step 3: Save Results
            # ---------------------------------------------------------------------------------------------------------

            # power flow for each line with reference bus0 or bus1
            p0 = self.network.lines_t.p0.to_numpy().reshape((-1,))                          # ref. bus 0
            p1 = self.network.lines_t.p1.to_numpy().reshape((-1,))                          # ref. bus 1

            # Create lines data frame
            df_lines = pd.DataFrame(data={'p0': p0, 'p1': p1},                              # init data frame
                                    index=self.network.lines_t.p0.columns)
            df_lines['name'] = df_lines.index                                               # index = line name

            df_lines['fromArea'] = [int(i.split('f')[1].split('t')[0])                      # line bus0
                                    for i in df_lines['name'].to_numpy(dtype=str)]

            df_lines['toArea'] = [int(i.split('t')[1].split('V')[0])                        # line bus1
                                  for i in df_lines['name'].to_numpy(dtype=str)]
            df_lines['voltage'] = [int(i.split('V')[1].split('_')[0])                       # voltage level
                                   for i in df_lines['name'].to_numpy(dtype=str)]

            df_lines['id'] = [i.split('_')[1]                                               # id = name
                              for i in df_lines['name'].to_numpy(dtype=str)]

            df_lines['s_nom'] = [i for i in self.network.lines['s_nom']]                    # total power

            df_lines.index = [time for _ in range(len(df_lines.index))]                     # Change index to timestamp

            self.connections['influxDB'].influx.write_points(dataframe=df_lines, measurement='Grid',
                                                             tag_columns=['name', 'fromArea', 'toArea',
                                                                          'voltage', 'id'])

            # Create buses dataframe
            power_bus = self.network.buses_t.p.to_numpy().reshape((-1,))
            df_buses = pd.DataFrame(data={'power_bus': power_bus},                          # init data frame
                                    index=self.network.buses_t.p.columns)

            df_buses['area'] = [int(i.split('_')[0])                                        # area name
                                for i in self.network.buses_t.p.columns.to_numpy(dtype=str)]

            df_buses['voltage'] = [int(i.split('_')[1])                                     # voltage level
                                   for i in self.network.buses_t.p.columns.to_numpy(dtype=str)]

            df_buses.index = [time for _ in range(len(df_buses.index))]                     # Change index to timestamp

            self.connections['influxDB'].influx.write_points(dataframe=df_buses, measurement='Grid',
                                                             tag_columns=['area', 'voltage'])

            time += pd.DateOffset(hours=1)


if __name__ == "__main__":

    agent = NetAgent(date='2018-01-01', plz=44)
    agent.connections['mongoDB'].login(agent.name)
    try:
        agent.run()
    except Exception as e:
        print(e)
    finally:
        agent.connections['mongoDB'].logout(agent.name)
        agent.connections['influxDB'].influx.close()
        agent.connections['mongoDB'].mongo.close()
        if not agent.connections['connectionMQTT'].is_closed:
            agent.connections['connectionMQTT'].close()
        exit()
