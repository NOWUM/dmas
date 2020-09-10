
class weatherForecast:

    def __init__(self, influx):

        self.influx = influx

    def forecast(self, geo, date, smooth):
        return self.influx.getWeather(geo, date, smooth)
