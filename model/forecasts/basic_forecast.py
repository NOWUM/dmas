# third party modules
from collections import deque
import pandas as pd
from pvlib.location import Location


from interfaces.simulation import SimulationInterface
from interfaces.weather import Weather


class BasicForecast:

    def __init__(self, position):
        self.input = deque(maxlen=1000)
        self.output = deque(maxlen=1000)
        self.counter = 0
        self.fitted = False
        self.simulation_database = SimulationInterface()
        self.weather_database = Weather()
        self.position = position
        self.location = Location(longitude=position['lon'], latitude=position['lat'])

    def collect_data(self, date):
        pass

    def forecast(self, date):
        pass

