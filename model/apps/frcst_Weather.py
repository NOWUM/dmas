
class weatherForecast:

    def __init__(self, influx, init=8):

        self.influx = influx
        self.collect = init         # days before a retrain is started
        self.counter = 0            # day counter

    def forecast(self, geo, date, mean):
        return self.influx.get_weather(geo, date, mean)

    def collect_data(self, date, dem, weather, prc, prc_1, prc_7):
        pass

    def fit_function(self):
        pass
