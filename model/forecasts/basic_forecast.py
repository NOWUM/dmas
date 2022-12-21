# third party modules
from collections import deque
from pvlib.location import Location


class BasicForecast:

    def __init__(self, position, simulation_interface, weather_interface):
        self.input = deque(maxlen=1000)
        self.output = deque(maxlen=1000)
        self.counter = 0
        self.fitted = False
        self.market = simulation_interface
        self.weather = weather_interface
        self.position = position
        self.location = Location(longitude=position['lon'], latitude=position['lat'])

    def collect_data(self, date):
        pass

    def forecast(self, date):
        pass
