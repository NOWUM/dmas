# third party modules
from collections import deque
from pvlib.location import Location


from interfaces.market import MarketInterface
from interfaces.weather import WeatherInterface


class BasicForecast:

    def __init__(self, position):
        self.input = deque(maxlen=1000)
        self.output = deque(maxlen=1000)
        self.counter = 0
        self.fitted = False
        self.market = MarketInterface()
        self.weather = WeatherInterface()
        self.position = position
        self.location = Location(longitude=position['lon'], latitude=position['lat'])

    def collect_data(self, date):
        pass

    def forecast(self, date):
        pass

