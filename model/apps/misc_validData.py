import os
os.chdir(os.path.dirname(os.path.dirname(__file__)))
from influxdb import InfluxDBClient
import pandas as pd

if __name__ == "__main__":

    influx = InfluxDBClient('149.201.88.150', 8086, 'root', 'root', 'MAS2020_1')
    influx.create_database('MASValidation')
    table = 1
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
                                   powerWindOnshore=validData.loc[index,'Wind Onshore[MW]'],
                                   powerWindOffshore=validData.loc[index, 'Wind Offshore [MW]'],
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