import os
os.chdir(os.path.dirname(os.path.dirname(__file__)))
from influxdb import InfluxDBClient
import pandas as pd
import numpy as np


def writeValidData(database, table):

    influx = InfluxDBClient('149.201.88.150', 8086, 'root', 'root', database)
    validData = pd.read_excel(r'./data/Valid_Data.xlsx', sheet_name=table)
    # validData.index = pd.date_range(start='2019-01-01', freq='15min', periods=len(validData))

    if table == 0:
        json_body = []
        dateRange = pd.date_range(start=pd.to_datetime('2019-01-01'), periods=35040, freq='15min')
        index = 0
        for date in dateRange:
            json_body.append(
                {
                    "measurement": "validation",
                    "tags": {},
                    "time": date.isoformat() + 'Z',
                    "fields": dict(powerWater=validData.loc[index, 'Laufwasser [MW]'],
                                   powerBio=validData.loc[index, 'Biomasse [MW]'],
                                   powerSolar=validData.loc[index, 'Solar [MW]'],
                                   powerWindOnshore=validData.loc[index,'Wind Onshore [MW]'],
                                   powerWindOffshore=validData.loc[index, 'Wind Offshore [MW]'],
                                   powerWind=validData.loc[index, 'Wind [MW]'],
                                   powerDemand=validData.loc[index,'Last [MW]'],
                                   powerNuc=validData.loc[index, 'Nuk [MW]'],
                                   powerCoal=validData.loc[index, 'Steinkohle [MW]'],
                                   powerLignite=validData.loc[index, 'Braunkohle [MW]'],
                                   powerGas=validData.loc[index, 'Gas [MW]'])
                }
            )
            index += 1
        influx.write_points(json_body)

    elif table == 1:
        json_body = []
        dateRange = pd.date_range(start=pd.to_datetime('2019-01-01'), periods=8760, freq='60min')
        index = 0
        for date in dateRange:
            json_body.append(
                {
                    "measurement": "validation",
                    "tags": {},
                    "time": date.isoformat() + 'Z',
                    "fields": dict(price=validData.loc[index, 'Preis'])
                }
            )
            index += 1
        influx.write_points(json_body)

def writeDayAheadError(database, date):

    influx = InfluxDBClient('149.201.88.150', 8086, 'root', 'root', database)

    print(date)

    # Tag im ISO Format
    date = pd.to_datetime(date)
    start = date.isoformat() + 'Z'
    end = (date + pd.DateOffset(days=1)).isoformat() + 'Z'
    # --> Abfrage
    query = 'select sum("price") from "DayAhead" where time >= \'%s\' and time < \'%s\' GROUP BY time(1h) fill(0)' \
            % (start, end)
    result = influx.query(query)
    if result.__len__() > 0:
        simulation = np.asarray([point['sum'] for point in result.get_points()])
    else:
        simulation = np.zeros(24)

    # --> Abfrage
    query = 'select sum("price") from "validation" where time >= \'%s\' and time < \'%s\' GROUP BY time(1h) fill(0)' \
            % (start, end)
    result = influx.query(query)
    if result.__len__() > 0:
        real = np.asarray([point['sum'] for point in result.get_points()])
    else:
        real = np.zeros(24)

    errorAbs = np.asarray([np.abs(-real[i] + simulation[i])/real[i] if real[i] != 0 else 0.05 for i in range(24)])
    errorNrm = np.asarray([(-real[i] + simulation[i])/real[i] if real[i] != 0 else 0.05 for i in range(24)])

    json_body = []
    time = date
    for i in range(24):
        json_body.append(
            {
                "measurement": 'validation',
                "time": time.isoformat() + 'Z',
                "fields": dict(errorMean=errorAbs[i],
                               errorNormal=errorNrm[i])
            }
        )
        time = time + pd.DateOffset(hours=1)

    influx.write_points(json_body)

    influx.close()


if __name__ == "__main__":

    for date in pd.date_range(start='2019-01-01', end='2019-12-31', freq='d'):
        writeDayAheadError('MAS2020_4', date)

    #writeValidData('MAS2020_3', 0)
    #writeValidData('MAS2020_3', 1)
