import requests
import pandas as pd
import io
import zipfile
import numpy as np
from interfaces.interface_mongo import mongoInterface
from shapely.geometry import shape, Point
from influxdb import InfluxDBClient

def getWind():

    mongoDB = mongoInterface('MAS_2019')
    shapes = []
# Get Geoshapes from Database
    for i in range(1,100):
        try:
            r = mongoDB.tableStructur.find_one({'_id': i})
            shapes.append((i, r['geojson'][0]))
        except:
            print('no shape found')


    ts = [[] for i in range(1, 101)]

# Get files from Stations which recorded till '20191231'
    url = "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/wind/historical/"
    r = requests.get(url)
    text = r.content.decode('utf-8')
    pos = 0
    stations = []
    toFind = 'stundenwerte'
    while pos != -1:
        pos = text.find(toFind, pos)
        if pos != -1:
            station = text[pos:pos+48]
            date = pd.to_datetime(station.split('_')[4])
            if date >= pd.to_datetime('20191231'):
                stations.append(text[pos:pos+48])
            pos += 1

# Get single file from Station unzip and open last file in zip archive.
    for station in stations:
        urlStation = url + station
        r = requests.get(urlStation, stream=True)
        if r.ok:
            try:
                z = zipfile.ZipFile(io.BytesIO(r.content))
                file = z.open(z.filelist[-1])
                content = file.read()
                file.close()
                # Prepare Data in values
                content = content.decode('utf-8')
                values = pd.read_csv(io.StringIO(content), sep=';')
                values['MESS_DATUM'] = pd.to_datetime(values['MESS_DATUM'].to_numpy(dtype=str), format='%Y%m%d%H')
                values = values.set_index('MESS_DATUM', drop=True)
                values = values.loc[np.logical_and(values.index >= pd.to_datetime('20190101'), values.index < pd.to_datetime('20200101'))]

# Open File 4 and extract geoshape
                file = z.open(z.filelist[4])
                content = file.read()
                file.close()
                content = content.decode('latin-1')
                position = pd.read_csv(io.StringIO(content), sep=';')
                # Try bc different formats in meta data from  DWD
                # Clear Dataframe from noise and norminate windspeed to 10m
                position = position[position['Stations_ID'] == np.unique(position['Stations_ID'])[0]]
                h = position['Geberhoehe ueber Grund [m]'].to_numpy()[-1]
                values['   F'] = values['   F'].replace(-999, 0)
                g = 0.2
                values['   F'] = values['   F'] * (10 / h) ** g
                values['   F'] = values['   F'].replace(0, -999)
                try:
                    br = position['Geogr.Breite'].to_numpy()[-1]
                    ln = position['Geogr.Laenge'].to_numpy()[-1]
                except:
                    br = position['Geo. Breite [Grad]'].to_numpy()[-1]
                    ln = position['Geo. Laenge [Grad]'].to_numpy()[-1]

                point = Point(ln, br)
                for e in shapes:
                    if shape(e[1]['geometry']).contains(point):
                        print(e[0])
                        ts[e[0]].append(values)
            except Exception as e:
                print(values.columns)
                print(position.columns)
                print(e)
                print('bad format in DWD-File')

# Calculating Mean
    counter = 1
    dict_ = {}
    for t in ts[1:]:

        if len(t) > 0:
            df = pd.DataFrame(columns=[str(i) for i in range(len(t))],
                              index=pd.date_range(start='2019-01-01', periods=8760, freq='60min'))
            index = 0
            for n in t:
                df.loc[:, str(index)] = n.loc[:, '   F']
            index += 1

            df = df.fillna(df.mean())
            df = df.replace(-999, df.mean())
            df = df.mean(axis=1)
            dict_.update({counter: df.to_numpy()})

        counter += 1


    influx = InfluxDBClient('149.201.88.150', 8086, 'root', 'root', 'weather')
    # Getting geohash
    geos = pd.read_excel('InfoGeo.xlsx', index_col=0)

    for key, value in dict_.items():
        json_body = []
        hash = (geos.loc[geos['PLZ'] == key, 'hash'].to_numpy()[0])
        dateRange = pd.date_range(start=pd.to_datetime('2019-01-01'), periods=8760, freq='60min')
        index = 0
        for date in dateRange:
            json_body.append(
                {
                    "measurement": "germany",
                    "tags": dict(geohash=hash),
                    "time": date.isoformat() + 'Z',
                    "fields": dict(Ws=value[index])
                }
            )

            index += 1
        influx.write_points(json_body)


def getRain():

    mongoDB = mongoInterface('MAS_XXXX')
    shapes = []
    for i in range(1, 100):
        try:
            r = mongoDB.tableStructur.find_one({'_id': i})
            shapes.append((i, r['geojson'][0]))
        except:
            print('no shape found')

    ts = [[] for i in range(1, 101)]

    url = "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/precipitation/historical/"
    r = requests.get(url)
    text = r.content.decode('utf-8')
    pos = 0
    stations = []
    toFind = 'stundenwerte'
    while pos != -1:
        pos = text.find(toFind, pos)
        if pos != -1:
            station = text[pos:pos + 48]
            date = pd.to_datetime(station.split('_')[4])
            if date >= pd.to_datetime('20191231'):
                stations.append(text[pos:pos + 48])
            pos += 1

    for station in stations:
        urlStation = url + station
        r = requests.get(urlStation, stream=True)
        if r.ok:
            try:
                z = zipfile.ZipFile(io.BytesIO(r.content))
                file = z.open(z.filelist[-1])
                content = file.read()
                file.close()
                content = content.decode('utf-8')
                values = pd.read_csv(io.StringIO(content), sep=';')
                values['MESS_DATUM'] = pd.to_datetime(values['MESS_DATUM'].to_numpy(dtype=str), format='%Y%m%d%H')
                values = values.set_index('MESS_DATUM', drop=True)
                values = values.loc[np.logical_and(values.index >= pd.to_datetime('20190101'),
                                                   values.index < pd.to_datetime('20200101'))]

                file = z.open(z.filelist[4])
                content = file.read()
                file.close()
                content = content.decode('latin-1')
                position = pd.read_csv(io.StringIO(content), sep=';')
                try:
                    br = position['Geogr.Breite'].to_numpy()[-1]
                    ln = position['Geogr.Laenge'].to_numpy()[-1]
                except:
                    br = position['Geo. Breite [Grad]'].to_numpy()[-1]
                    ln = position['Geo. Laenge [Grad]'].to_numpy()[-1]

                point = Point(ln, br)
                for e in shapes:
                    if shape(e[1]['geometry']).contains(point):
                        print(e[0])
                        ts[e[0]].append(values)
            except Exception as e:
                print(values.columns)
                print(position.columns)
                print(e)
                print('bad format in DWD-File')

    counter = 1
    dict_ = {}
    for t in ts[1:]:

        if len(t) > 0:
            df = pd.DataFrame(columns=[str(i) for i in range(len(t))],
                              index=pd.date_range(start='2019-01-01', periods=8760, freq='60min'))
            index = 0
            for n in t:
                df.loc[:, str(index)] = n.loc[:, '   F']
            index += 1

            df = df.fillna(df.mean())
            df = df.replace(-999, df.mean())
            df = df.mean(axis=1)
            dict_.update({counter: df.to_numpy()})

        counter += 1

    influx = InfluxDBClient('149.201.88.150', 8086, 'root', 'root', 'weather')
    geos = pd.read_excel('InfoGeo.xlsx', index_col=0)

    for key, value in dict_.items():
        json_body = []
        hash = (geos.loc[geos['PLZ'] == key, 'hash'].to_numpy()[0])
        dateRange = pd.date_range(start=pd.to_datetime('2019-01-01'), periods=8760, freq='60min')
        index = 0
        for date in dateRange:
            json_body.append(
                {
                    "measurement": "germany",
                    "tags": dict(geohash=hash),
                    "time": date.isoformat() + 'Z',
                    "fields": dict(Ws=value[index])
                }
            )

            index += 1
        influx.write_points(json_body)


if __name__ == "__main__":
    getWind()

    # mongoDB = mongoInterface('MAS_XXXX')
    # shapes = []
    # for i in range(1, 100):
    #     try:
    #         r = mongoDB.tableStructur.find_one({'_id': i})
    #         shapes.append((i, r['geojson'][0]))
    #     except:
    #         print('no shape found')
    #
    # ts = [[] for i in range(1, 101)]
    #
    # url = "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/precipitation/historical/"
    # r = requests.get(url)
    # text = r.content.decode('utf-8')
    # pos = 0
    # stations = []
    # toFind = 'stundenwerte'
    # while pos != -1:
    #     pos = text.find(toFind, pos)
    #     if pos != -1:
    #         station = text[pos:pos + 48]
    #         date = pd.to_datetime(station.split('_')[4])
    #         if date >= pd.to_datetime('20191231'):
    #             stations.append(text[pos:pos + 48])
    #         pos += 1
    #
    # for station in stations:
    #     urlStation = url + station
    #     r = requests.get(urlStation, stream=True)
    #     if r.ok:
    #         try:
    #             z = zipfile.ZipFile(io.BytesIO(r.content))
    #             file = z.open(z.filelist[-1])
    #             content = file.read()
    #             file.close()
    #             content = content.decode('utf-8')
    #             values = pd.read_csv(io.StringIO(content), sep=';')
    #             values['MESS_DATUM'] = pd.to_datetime(values['MESS_DATUM'].to_numpy(dtype=str), format='%Y%m%d%H')
    #             values = values.set_index('MESS_DATUM', drop=True)
    #             values = values.loc[np.logical_and(values.index >= pd.to_datetime('20190101'),
    #                                                values.index < pd.to_datetime('20200101'))]
    #
    #             file = z.open(z.filelist[4])
    #             content = file.read()
    #             file.close()
    #             content = content.decode('latin-1')
    #             position = pd.read_csv(io.StringIO(content), sep=';')
    #             try:
    #                 br = position['Geogr.Breite'].to_numpy()[-1]
    #                 ln = position['Geogr.Laenge'].to_numpy()[-1]
    #             except:
    #                 br = position['Geo. Breite [Grad]'].to_numpy()[-1]
    #                 ln = position['Geo. Laenge [Grad]'].to_numpy()[-1]
    #
    #             point = Point(ln, br)
    #             for e in shapes:
    #                 if shape(e[1]['geometry']).contains(point):
    #                     print(e[0])
    #                     ts[e[0]].append(values)
    #         except Exception as e:
    #             print(values.columns)
    #             print(position.columns)
    #             print(e)
    #             print('bad format in DWD-File')
    #
    # counter = 1
    # dict_ = {}
    # for t in ts:
    #
    #     if len(t) > 0:
    #         df = pd.DataFrame(columns=[str(i) for i in range(len(t))],
    #                           index=pd.date_range(start='2019-01-01', periods=8760, freq='60min'))
    #         index = 0
    #         for n in t:
    #             df.loc[:, str(index)] = n.loc[:, '  R1']
    #         index += 1
    #
    #         df = df.fillna(df.mean())
    #         df = df.replace(-999, df.mean())
    #         df = df.mean(axis=1)
    #         dict_.update({counter: df.to_numpy()})
    #
    #     counter += 1
    #
    # influx = InfluxDBClient('149.201.88.150', 8086, 'root', 'root', 'weather')
    # geos = pd.read_excel('InfoGeo.xlsx', index_col=0)
    #
    # for key, value in dict_.items():
    #     json_body = []
    #     hash = (geos.loc[geos['PLZ'] == key, 'hash'].to_numpy()[0])
    #     dateRange = pd.date_range(start=pd.to_datetime('2019-01-01'), periods=8760, freq='60min')
    #     index = 0
    #     for date in dateRange:
    #         json_body.append(
    #             {
    #                 "measurement": "germany",
    #                 "tags": dict(geohash=hash),
    #                 "time": date.isoformat() + 'Z',
    #                 "fields": dict(Nd=value[index])
    #             }
    #         )
    #
    #         index += 1
    #     influx.write_points(json_body)