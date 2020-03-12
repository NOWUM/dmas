import pandas as pd
import requests
import numpy as np
import json
import pypsa
from agents.interface import Interface


class netAgent(Interface):

    def __init__(self, date=pd.to_datetime('01.01.2019'), plz=1, host='149.201.88.150'):
        super().__init__(date=date, plz=plz, host=host, exchange='DayAhead', typ='RES')

        print('Start Building Grid')
        self.network = pypsa.Network()
        buses = pd.read_csv(r'./grid/buses.csv', sep=';', decimal=',', index_col=0)
        lines = pd.read_csv(r'./grid/lines.csv', sep=';', decimal=',', index_col=0)
        transformers = pd.read_csv(r'./grid/transformers.csv', sep=';', decimal=',', index_col=0)

        self.network.import_components_from_dataframe(buses, 'Bus')
        self.network.import_components_from_dataframe(lines, 'Line')
        self.network.import_components_from_dataframe(transformers, 'Transformer')

        self.network.consistency_check()

        print('Stop Building Grid')

if __name__ == "__main__":

    date = pd.to_datetime('01.01.2019')

    agent = netAgent(date=date)
    start = date.isoformat() + 'Z'
    end = (date + pd.DateOffset(days=1)).isoformat() + 'Z'

    query = 'SELECT sum("P") FROM "PWP" WHERE ("timestamp" = \'actual\') and time >= \'%s\' and time < \'%s\' GROUP BY time(1h) fill(0)' %(start,end)
    result = agent.influx.query(query)
    generationPWP = np.asarray([np.round(point['sum'],2) for point in result.get_points()])

    query = 'SELECT sum("P") FROM "RES" WHERE ("timestamp" = \'forecast\') and time >= \'%s\' and time < \'%s\' GROUP BY time(1h) fill(0)' %(start,end)
    result = agent.influx.query(query)
    generationRES1 = np.asarray([np.round(point['sum'],2) for point in result.get_points()])

    query = 'SELECT sum("P") FROM "RES" WHERE ("timestamp" = \'actual\') and time >= \'%s\' and time < \'%s\' GROUP BY time(1h) fill(0)' %(start,end)
    result = agent.influx.query(query)
    generationRES = generationRES1 + np.asarray([np.round(point['sum'],2) for point in result.get_points()])

    query = 'SELECT sum("P") FROM "DEM" WHERE ("timestamp" = \'forecast\') and time >= \'%s\' and time < \'%s\' GROUP BY time(1h) fill(0)' %(start,end)
    result = agent.influx.query(query)
    demand1 = np.asarray([np.round(point['sum'],2) for point in result.get_points()])

    query = 'SELECT sum("P") FROM "DEM" WHERE ("timestamp" = \'actual\') and time >= \'%s\' and time < \'%s\' GROUP BY time(1h) fill(0)' %(start,end)
    result = agent.influx.query(query)
    demand = demand1 + np.asarray([np.round(point['sum'],2) for point in result.get_points()])