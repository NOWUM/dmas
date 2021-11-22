# third party modules
import os
import time as tme
import pandas as pd
import numpy as np
import calendar

# model modules
from agents.basic_Agent import BasicAgent
from interfaces.weather import Weather

class WtrAgent(BasicAgent):

    def __init__(self, date, plz, mqtt_exchange, simulation_database):
        super().__init__(date, plz, 'CTL', mqtt_exchange, simulation_database)
        self.interface_weather = Weather()
        self.geo_info = pd.read_csv(r'./data/Ref_GeoInfo.csv', index_col=0, sep=';', decimal=',')
        self.geo_info = self.geo_info.set_index('hash')

        self.sim_date = pd.to_datetime(self.sim_year, format='%y')
        self.year = self.date.year
        self.leap_years = set([i for i in range(1996, 2016, 4)])
        self.norm_years = set([i for i in range(1995, 2016)]).difference(self.leap_years)
        if calendar.isleap(pd.to_datetime(date).year):
            self.sim_year = np.random.choice(list(self.leap_years))
        else:
            self.sim_year = np.random.choice(list(self.norm_years))

    def callback(self, ch, method, properties, body):
        message = body.decode("utf-8")
        self.date = pd.to_datetime(message.split(' ')[1])
        # if new year
        if self.date.year != self.year:
            self.year = self.date.year
            if calendar.isleap(self.date.year):
                self.sim_year = np.random.choice(list(self.leap_years))
            else:
                self.sim_year = np.random.choice(list(self.norm_years))

        self.sim_date = pd.to_datetime(f'{self.sim_year}{self.date.month}{self.date.days_in_month}', format='%y%m%d')
        # Call for Market Clearing
        # -----------------------------------------------------------------------------------------------------------
        if 'calculate_weather' in message:
            try:
                self.set_weather()
            except:
                self.logger.exception('Error while calculating')
        # Terminate Agent
        # -----------------------------------------------------------------------------------------------------------
        if 'kill' in message or self.name in message or self.typ + '_all' in message:
            if not self.mqtt_connection.is_closed:
                self.mqtt_connection.close()

    def set_weather(self):
        pass


if __name__ == "__main__":
    test = WtrAgent(date='2019-01-01', plz=4, mqtt_exchange='dMAS', simulation_database='dMAS')


