from math import sin, cos, sqrt, atan2, radians, pi
import pymongo
import numpy as np
import pandas as pd
import pyproj
import shapely.ops as ops
from shapely.geometry.polygon import Polygon
from functools import partial


if __name__ == "__main__":
    # approximate radius of earth in km
    R = 6373.0

    mongo = pymongo.MongoClient('mongodb://' + '149.201.88.150' + ':27017/')
    structDB = mongo["testDB"]

    df = pd.read_csv(r'./data/toBuildData/windMean.csv', sep=';', index_col=0, decimal=',')
    distancesAll = []
    useDistance = []
    T = []
    index = []
    for i in range(1, 100):

        test = structDB['PLZ_%s' % i]
        try:
            point = test.find_one({"_id": 'Position'})
            latRef = radians(point['lat'])
            lonRef = radians(point['lon'])

            geoJson = test.find_one({"_id": 'geoJson'})
            if geoJson['geometry']['type'] == 'Polygon':
                points = []
                for coordinates in geoJson['geometry']['coordinates']:
                    for point in coordinates:
                        tmp = point
                        tmp.append(0)
                        points.append(tmp)
            else:
                points = []
                for element in geoJson['geometry']['coordinates']:
                    for point in element[0]:
                        tmp = point
                        tmp.append(0)
                        points.append(tmp)

            distances = []
            distancesRad = []

            geom = Polygon(points)

            geom_area = ops.transform(
                partial(
                    pyproj.transform,
                    pyproj.Proj(init='EPSG:4326'),
                    pyproj.Proj(
                        proj='aea',
                        lat_1=geom.bounds[1],
                        lat_2=geom.bounds[3])),
                geom)

            x = np.sqrt(geom_area.area/pi)/1000

            for point in points:

                lat = radians(point[1])
                lon = radians(point[0])
                dlon = lat - latRef
                dlat = lon - lonRef

                a = sin(dlat / 2)**2 + cos(latRef) * cos(lat) * sin(dlon / 2)**2
                c = 2 * atan2(sqrt(a), sqrt(1 - a))

                distance = R * c

                distances.append(distance)
                distancesRad.append(c)

            # print(np.mean(distances))
            # allAreas.append(np.mean(distances)**2*pi)

            distancesAll.append([np.mean(distances), x])
            useDistance.append(2*x)

            T.append((1000*x/df.loc[df.index == i]['Ws'].to_numpy()[0]))
            index.append(i)

        except Exception as e:
            print(i)
            print(e)

    distancesAll = np.asarray(distancesAll)
    N = [int(t/3600) for t in T]
    toSave = pd.DataFrame(index=index, data=N, columns=['smooth factor'])