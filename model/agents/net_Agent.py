import os
os.chdir(os.path.dirname(os.path.dirname(__file__)))
import pandas as pd
import numpy as np
import pypsa
from plotly.colors import label_rgb, find_intermediate_color
import json
import plotly

from agents.basic_Agent import agent as basicAgent

from interfaces.interface_Influx import InfluxInterface #temp #TODO: only used for standalone testing --> remove after standalone testing/if not used anymore

class NetAgent(basicAgent):

    def __init__(self, date, plz):
        super().__init__(date=date, plz=plz, typ='NET')

        # create pypsa instance for powerflow
        self.network = pypsa.Network()

        # load grid data
        print('Reading Data from CSV...')
        # read Excel:
        #buses = pd.read_excel(r'./data/Grid_Buses.xlsx', index_col=0)
        #lines = pd.read_excel(r'./data/Grid_Lines.xlsx', index_col=0)
        #transformers = pd.read_excel(r'./data/Grid_Trafos.xlsx', index_col=0)

        # read CSV (faster):
        buses = pd.read_csv(r'./data/Grid_Buses.csv', sep=';', decimal=',', index_col=0)
        lines = pd.read_csv(r'./data/Grid_Lines.csv', sep=';', decimal=',', index_col=0)
        transformers = pd.read_csv(r'./data/Grid_Trafos.csv', sep=';', decimal=',', index_col=0)

        # build grid
        print('Start Building Grid...')
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

        # TODO: Auswahlmöglichkeit für Schiene, sodass realistische Verteilung auf 380 und 220 kV Schiene möglich ist (momentan wird immer 380kv genommen, wenn vorhanden, und 220 dann ignoriert)
        for i in range(1, 100):
            if '%s_380' % i in buses.to_numpy() or '%s_220' % i in buses.to_numpy() and i not in [5, 11, 20, 43, 60, 62, 70, 80]:
                if '%s_380' % i in buses.to_numpy():
                    self.network.add("Load", 'Load_%s' % i, p_set=0, bus='%s_380' % i)
                    self.network.add("Generator", 'Gen_%s' % i, p_set=0, control='PV', bus='%s_380' % i)
                else:
                    self.network.add("Load", 'Load_%s' % i, p_set=0, bus='%s_220' % i)
                    self.network.add("Generator", 'Gen_%s' % i, p_set=0, control='PV', bus='%s_220' % i)

        print('Stop Building Grid')

    def calc_power_flow(self): # Hier findet die Netzberechnung statt
        # 1. Power aller Areas aus Influx abrufen
        # 2. Lastfluss berechnen
        # 3. Ergebnis Lastfluss in Datenbank speichern

        print('Start calculating Powerflow for', self.date, '...')

        # Step 1: Get power data from database
        power_total = [riekeInfluxInterface.get_power_area(date=self.date, area=i) for i in range(1,100)] #using riekeInfluxInterface for testing TODO: use default interface (self.connections['influxDB'].) for get_power_area
        #power_total = [self.connections['influxDB'].get_power_area(date=self.date, area=i) for i in range(1, 100)] #TODO: use this line after testing
        #print('power_total:', power_total[0]) # test print

        # Step 2: Set data for grid calculation
        #result_df = pd.DataFrame() #"Never grow a DataFrame! It is always cheaper to append to a python list and then convert it to a DataFrame at the end, both in terms of memory and performance. – cs95 Feb 29 at 12:04"
        result_list = [] # Liste, die später mit allen den Ergebnissen des Powerflows für 24h eines Tages gefüllt wird
        time = self.date
        for hour in range (0,24):#für 24h am Tag
            index = 1
            for row in np.asarray(power_total): #für alle PLZ-Gebiete
               if index not in [5, 11, 20, 43, 60, 62, 70, 80]: #auszulassende PLZ-Gebiete
                   # Last und Erzeugung setzen
                   load = [val if val >= 0 else 0 for val in row] #Lasten setzen, wenn Bilanz positiv (sonst ist es ein Erzeuger und Wert wird auf 0 gesetzt)
                   gen = [-1 * val if val < 0 else 0 for val in row] # Erzeugung (absolut) setzen, wenn Bilanz negativ (0 wenn Last)
                   self.network.loads.loc['Load_%s' % index, 'p_set'] = load[hour]#TODO: für alle 24h
                   self.network.generators.loc['Gen_%s' % index, 'p_set'] = gen[hour]#TODO: für alle 24h
               index += 1

            self.network.pf(distribute_slack=True) # Lastfluss berechnen

            result_list.append(self.network.lines_t.p0.loc['now'])
            #print('Powerflow calculation hour {hour:d} done'.format(hour=hour))

            #df_lines = self.network.lines_t.p0 # copy results from powerflow into new dataframe
            #df_lines_transpose = pd.DataFrame(df_lines.transpose()) # switch columns and rows

            #p0 = self.network.lines_t.p0.to_numpy()  # copy results from powerflow into new dataframe
            #p1 = self.network.lines_t.p1.to_numpy()  # copy results from powerflow into new dataframe
            #self.network.lines_t.p1.transpose()['now']

            # Create lines dataframe
            p0 = self.network.lines_t.p0.to_numpy().reshape((-1,))
            p1 = self.network.lines_t.p1.to_numpy().reshape((-1,))
            df_lines_transpose = pd.DataFrame(data={'p0': p0, 'p1': p1}, index=self.network.lines_t.p0.columns)
            # Create additional columns for lines dataframe
            df_lines_transpose['name'] = df_lines_transpose.index # index (Name der Leitung) in Spalte name kopieren
            df_lines_transpose['fromArea'] = [int(i.split('f')[1].split('t')[0]) for i in df_lines_transpose['name'].to_numpy(dtype=str)]
            df_lines_transpose['toArea'] = [int(i.split('t')[1].split('V')[0]) for i in df_lines_transpose['name'].to_numpy(dtype=str)]
            df_lines_transpose['voltage'] = [int(i.split('V')[1].split('_')[0]) for i in df_lines_transpose['name'].to_numpy(dtype=str)]
            df_lines_transpose['id'] = [i.split('_')[1] for i in df_lines_transpose['name'].to_numpy(dtype=str)]
            df_lines_transpose['s_nom'] = [i for i in self.network.lines['s_nom']]

            # # df_lines_transpose['fromArea'] = [int(i.split('t',1)[0].replace('f','')) for i in df_lines_transpose['name'].to_numpy(dtype=str)] # Alternative, fromArea aus name auszulesen

            # # rename column "now" to "power"
            # df_lines_transpose.rename(columns={"now": "power_p0"}, inplace=True)  # spalte "now" in "power" umbenennen; inplace=True damit altes DF überschrieben wird!?
            # # testDFtranspose.columns = ['power','name','...'] #harte Überschreibung der Spaltennamen

            # change index to timestamp
            df_lines_transpose.index = [time for _ in range(len(df_lines_transpose.index))] # index auf Zeit setzen (Datum mit aktueller Stunde)

            # Create buses dataframe
            power_bus = self.network.buses_t.p.to_numpy().reshape((-1,))
            df_buses_transpose = pd.DataFrame(data={'power_bus': power_bus}, index=self.network.buses_t.p.columns)
            df_buses_transpose.index = [time for _ in range(
                len(df_buses_transpose.index))]  # index auf Zeit setzen (Datum mit aktueller Stunde)
            df_buses_transpose['area'] = [int(i.split('_')[0]) for i in
                                          self.network.buses_t.p.columns.to_numpy(dtype=str)]
            df_buses_transpose['voltage'] = [int(i.split('_')[1]) for i in
                                             self.network.buses_t.p.columns.to_numpy(dtype=str)]

            # Step 3: save dataframe to database #TODO: change database to database in config after testing:
            tobiInfluxInterface.influx.write_points(dataframe=df_lines_transpose, measurement='Grid', tag_columns=['name', 'fromArea', 'toArea', 'voltage', 'id'])# TODO: remove this line after testing
            #self.connections['influxDB'].influx.write_points(dataframe=df_lines_transpose, measurement='Grid', tag_columns=['name', 'fromArea', 'toArea', 'voltage', 's_nom', 'id'])# TODO: use this line after testing


            tobiInfluxInterface.influx.write_points(dataframe=df_buses_transpose, measurement='Grid', tag_columns=['area', 'voltage'])  # TODO: remove this line after testing

            # print status
            print('Powerflow for', time, 'wrote to database')

            # next hour
            time += pd.DateOffset(hours=1)


        # (old/unused: moved into loop above) Step 3: Build Dataframe to save results in influxDB/MongoDB #moved into loop
        result_df = pd.DataFrame(result_list)
        #result_df.index = [i for i in range(len(result_df))] #TypeError: index is not a valid DatetimeIndex or PeriodIndex
        result_df.index = [self.date + pd.DateOffset(hours=i) for i in range(len(result_df.index))]

        # self.connections['influxDB'].save_data(df, 'Grid')                --> Variante 1
        # self.connections['influxDB'].influx.write_points(dataframe=df,    --> Variante 2
        # measurement='Grid', tag_columns=['names', 'order', 'typ'])
        #tobiInfluxInterface.save_data(result_df, 'Grid') #klappt, Daten stehen in Datenbank und sind in Grafana abrufbar

        print('Stop calculating Powerflow')
        pass

if __name__ == "__main__":
    # args = parse_args()
    agent = NetAgent(date='2018-01-02', plz=44) #TODO: NetAgent should not require "plz"
    riekeInfluxInterface = InfluxInterface(database='MAS2020_10')# Influx Connection used to load data for powerflow
    tobiInfluxInterface = InfluxInterface(database='MAS2020_TobiTest')# Influx Connection used to write data during testing, so actual database does not get overwritten
    agent.calc_power_flow() #TODO:this line is only for standalone testing, remove line if callback from main is running
    #agent.connections['mongoDB'].login(agent.name, False)#nur benötigt, wenn Agent am Markt teilnehmen soll
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
