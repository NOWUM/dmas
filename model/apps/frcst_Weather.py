import pandas as pd


class weatherForecast:

    def __init__(self, influx):

        self.influx = influx

    def forecast(self, geo, date):

        return self.influx.getWeather(geo, date)