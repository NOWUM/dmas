import pandas as pd

class weatherForecast:

    def __init__(self, influx):

        self.influx = influx

    def forecast(self,geo,date):

        geohash = str(geo)                                              # -- PLZ-Information
        # -- Build-up query for InfluxDB
        start = date.isoformat() + 'Z'
        end = (date + pd.DateOffset(days=1)).isoformat() + 'Z'
        query = 'select * from "weather" where time >= \'%s\' and time <= \'%s\' and geohash = \'%s\'' % (start, end, geohash)
        result = self.influx.query(query)
        # -- Build Parameter
        dir = [point['DNI'] for point in result.get_points()]           # -- direct irradiation     [W/m²]
        dif = [point['DHI'] for point in result.get_points()]           # -- diffuse irradiation    [W/m²]
        wind = [point['Ws'] for point in result.get_points()]           # -- windspeed              [m/s] (2m)
        TAmb = [point['TAmb'] for point in result.get_points()]         # -- temperatur             [°C]

        return dict(wind=wind, dir=dir, dif=dif, temp=TAmb)