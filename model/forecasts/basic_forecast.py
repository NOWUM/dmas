from collections import deque
from interfaces.simulation import SimulationInterface
from interfaces.weather import Weather


class BasicForecast:

    def __init__(self):
        self.input = deque(maxlen=1000)
        self.output = deque(maxlen=1000)
        self.counter = 0
        self.fitted = False
        self.simulation_database = SimulationInterface()
        self.weather_database = Weather()

    def collect_data(self, date):
        pass

    def forecast(self, date):
        pass

