import numpy as np
from datetime import date, timedelta
from dateutil.easter import easter


def get_holidays(year):
    easter_day = easter(year)

    # -- holidays in germany
    holidays = []
    holidays.append(easter_day)
    holidays.append(easter_day - timedelta(days=2))                 # -- Karfreitag
    holidays.append(easter_day + timedelta(days=1))                 # -- Ostermontag
    holidays.append(easter_day + timedelta(days=39))                # -- Christihimmelfahrt
    holidays.append(easter_day + timedelta(days=49))                # -- Pfingstsonntag
    holidays.append(easter_day + timedelta(days=50))                # -- Pfingstmontag
    holidays.append(easter_day + timedelta(days=60))                # -- Fronleichnam
    holidays.append(date(year, 12, 24))                         # -- 1. Weihnachtstag
    holidays.append(date(year, 12, 25))                         # -- 1. Weihnachtstag
    holidays.append(date(year, 12, 26))                         # -- 2. Weihnachtstag
    holidays.append(date(year, 12, 31))                         # -- 2. Weihnachtstag
    holidays.append(date(year, 1, 1))                           # -- Neujahr
    holidays.append(date(year, 5, 1))                           # -- 1. Mai
    holidays.append(date(year, 10, 3))                          # -- Tag der deutschen Einheit

    return np.asarray([h.timetuple().tm_yday for h in holidays])


profiles = {
    'household': np.asarray(np.load(open(r'./demandlib/data/household.pkl', 'rb'))),
    'business': np.asarray(np.load(open(r'./demandlib/data/business.pkl', 'rb'))),
    'industry': np.asarray(np.load(open(r'./demandlib/data/industry.pkl', 'rb'))),
    'agriculture': np.asarray(np.load(open(r'./demandlib/data/agriculture.pkl', 'rb'))),
}

winter = np.asarray(np.load(open(r'./demandlib/data/winter.pkl', 'rb')))
summer = np.asarray(np.load(open(r'./demandlib/data/summer.pkl', 'rb')))


class StandardLoadProfile:

    def __init__(self, demandP, type='household', hourly=True):

        self.type = type
        self.demandP = demandP
        self.profile = profiles[type]

        self.winter = winter
        self.summer = summer

        self.hourly = hourly

    def run_model(self, d):
        '''
        returns the load profile for a given day in [kW]
        '''

        doy = d.dayofyear
        dow = d.dayofweek
        year = d.year

        f = self.demandP / 1e6  # [kW] -> [GWh/a]
        if self.type == 'household':
            f *= -0.000000000392 * doy ** 4 + 0.00000032 * doy ** 3 - 0.0000702 * doy ** 2 + 0.0021 * doy + 1.24

        demand = np.zeros(96)

        if doy in self.summer:
            if dow == 6 or doy in get_holidays(year):
                demand = self.profile[:, 4] * f
            elif dow < 5:
                demand = self.profile[:, 5] * f
            elif dow == 5:
                demand = self.profile[:, 3] * f
        elif doy in self.winter:
            if dow == 6 or doy in get_holidays(year):
                demand = self.profile[:, 1] * f
            elif dow < 5:
                demand = self.profile[:, 2] * f
            elif dow == 5:
                demand = self.profile[:, 0] * f
        else:
            if dow == 6 or doy in get_holidays(year):
                demand = self.profile[:, 7] * f
            elif dow < 5:
                demand = self.profile[:, 8] * f
            elif dow == 5:
                demand = self.profile[:, 6] * f

        if self.hourly:
            return np.asarray([np.mean(demand[i:i + 3]) for i in range(0, 96, 4)], np.float).reshape((-1,))
        else:
            return demand

if __name__ == '__main__':
    s = StandardLoadProfile(1000)
    import pandas as pd

    dd = pd.to_datetime('2021-12-12')
    summe = s.run_model(dd)
    for i in range(365):
        summe += s.run_model(dd)
    print(summe.sum())