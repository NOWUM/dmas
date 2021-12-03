# third party modules
import pandas as pd
import numpy as np
import calendar
from datetime import date as dt

# model modules
from agents.basic_Agent import BasicAgent
from interfaces.weather import Weather


class WtrAgent(BasicAgent):

    def __init__(self, date, plz, agent_type, mqtt_exchange, connect,  infrastructure_source, infrastructure_login):
        super().__init__(date, plz, agent_type, mqtt_exchange, connect, infrastructure_source, infrastructure_login)

        self.sim_date = dt(1995, 1, 1)
        self.year = self.date.year
        self.leap_years = set([i for i in range(1996, 2016, 4)])
        self.norm_years = set([i for i in range(1995, 2016)]).difference(self.leap_years)
        if calendar.isleap(pd.to_datetime(date).year):
            self.sim_year = np.random.choice(list(self.leap_years))
        else:
            self.sim_year = np.random.choice(list(self.norm_years))

    def callback(self, ch, method, properties, body):
        super().callback(ch, method, properties, body)
        message = body.decode("utf-8")
        self.date = pd.to_datetime(message.split(' ')[1])
        # if new year
        if self.date.year != self.year:
            self.year = self.date.year
            if calendar.isleap(self.date.year):
                self.sim_year = np.random.choice(list(self.leap_years))
            else:
                self.sim_year = np.random.choice(list(self.norm_years))

        self.sim_date = dt(self.sim_year, self.date.month, int(self.date.days_in_month))
        # Call for Market Clearing
        # -----------------------------------------------------------------------------------------------------------
        if 'calculate_weather' in message:
            try:
                self.set_weather()
            except Exception:
                self.logger.exception('Error while calculating')

    def set_weather(self):
        self.channel.basic_publish(exchange=self.exchange, routing_key='', body=f'weather_date {self.sim_date}')
