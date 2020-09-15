import os
os.chdir(os.path.dirname(os.path.dirname(__file__)))
from influxdb import InfluxDBClient, DataFrameClient
import pandas as pd
import numpy as np


def write_valid_data(database, table, start, end):

    client = DataFrameClient('149.201.88.150', 8086, 'root', 'root', database)
    protocol = 'line'
    start = pd.to_datetime(start, utc=True)
    end = pd.to_datetime(end, utc=True)

    if table == 0:
        valid_data = pd.read_pickle(r'./data/validGeneration.pkl')
        valid_data.index = pd.date_range(start='2018-01-01', end='2019-12-31 23:45:00', freq='15min', tz='CET')
        valid_data = valid_data.loc[np.logical_and(start <= valid_data.index, valid_data.index <= end), :]
        client.write_points(valid_data, 'validation', protocol=protocol)

    elif table == 1:
        valid_data = pd.read_pickle(r'./data/validPrice.pkl')
        valid_data.index = pd.date_range(start='2018-01-01', end='2019-12-31 23:00:00', freq='60min', tz='UTC')
        valid_data = valid_data.loc[np.logical_and(start <= valid_data.index, valid_data.index <= end), :]
        client.write_points(valid_data, 'validation', protocol=protocol)

    elif table == 2:
        valid_data = pd.read_pickle(r'./data/validCapacity.pkl')
        valid_data.index = pd.date_range(start='2018-01-01', end='2019-12-31 23:45:00', freq='15min', tz='CET')
        valid_data = valid_data.loc[np.logical_and(start <= valid_data.index, valid_data.index <= end), :]
        client.write_points(valid_data, 'validation', protocol=protocol)

def writeDayAheadError(database, date):

    client = DataFrameClient('149.201.88.150', 8086, 'root', 'root', database)
    protocol = 'line'

    # collect simulation data
    query = 'select sum("price") as "price" from "DayAhead" where time >= \'%s\' and time < \'%s\' GROUP BY time(1h)'\
            % (pd.to_datetime(date).isoformat() + 'Z', (pd.to_datetime(date)+pd.DateOffset(days=1)).isoformat() + 'Z')
    result = client.query(query)
    if result.__len__() > 0:
        simulation = ['DayAhead']['price'].to_numpy()
    else:
        simulation = np.zeros(24)

    # collect original data
    query = 'select sum("price") as "price" from "validation" where time >= \'%s\' and time < \'%s\' GROUP BY time(1h)'\
            % (pd.to_datetime(date).isoformat() + 'Z', (pd.to_datetime(date)+pd.DateOffset(days=1)).isoformat() + 'Z')
    result = client.query(query)
    if result.__len__() > 0:
        real = ['validation']['price'].to_numpy()
    else:
        real = np.zeros(24)

    # claculate error and save results
    df = pd.DataFrame(index=pd.to_datetime(date), data={'mse': np.mean((real-simulation)**2),
                                                        'me': np.mean(real-simulation)})
    client.write_points(df, 'validation', protocol=protocol)
    client.close()


if __name__ == "__main__":

    # writeValidData('MAS2020_9', 0)

    write_valid_data('MAS2020_12', 0, start='2018-01-01', end='2019-01-01')
    write_valid_data('MAS2020_12', 1, start='2018-01-01', end='2019-01-01')
    write_valid_data('MAS2020_12', 2, start='2018-01-01', end='2019-01-01')

    # for date in pd.date_range(start='2019-01-01', end='2019-12-31', freq='d'):
    #     writeDayAheadError('MAS2020_6', date)