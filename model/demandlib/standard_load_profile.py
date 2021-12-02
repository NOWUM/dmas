import numpy as np
from datetime import date, timedelta


def get_holidays(year):
    # -- get eastern
    a = year % 19
    b = year // 100
    c = year % 100
    d = (19 * a + b - b // 4 - ((b - (b + 8) // 25 + 1) // 3) + 15) % 30
    e = (32 + 2 * (b % 4) + 2 * (c // 4) - d - (c % 4)) % 7
    f = d + e - 7 * ((a + 11 * d + 22 * e) // 451) + 114
    month = f // 31
    day = f % 31 + 1
    easter = date(year, month, day)

    # -- holidays in germany
    holidays = []
    holidays.append(easter)
    holidays.append(easter - timedelta(days=2))                 # -- Karfreitag
    holidays.append(easter + timedelta(days=1))                 # -- Ostermontag
    holidays.append(easter + timedelta(days=39))                # -- Christihimmelfahrt
    holidays.append(easter + timedelta(days=49))                # -- Pfingstsonntag
    holidays.append(easter + timedelta(days=50))                # -- Pfingstmontag
    holidays.append(easter + timedelta(days=60))                # -- Fronleichnam
    holidays.append(date(year, 12, 24))                         # -- 1. Weihnachtstag
    holidays.append(date(year, 12, 25))                         # -- 1. Weihnachtstag
    holidays.append(date(year, 12, 26))                         # -- 2. Weihnachtstag
    holidays.append(date(year, 12, 31))                         # -- 2. Weihnachtstag
    holidays.append(date(year, 1, 1))                           # -- Neujahr
    holidays.append(date(year, 5, 1))                           # -- 1. Mai
    holidays.append(date(year, 10, 3))                          # -- Tag der deutschen Einheit
    holidays.append(date(year, 10, 31))                         # -- Reformationstag

    return np.asarray([h.timetuple().tm_yday for h in holidays])


profiles = {
    'household': np.asarray(np.load(open(r'./demandlib/household.pkl', 'rb'))),
    'business': np.asarray(np.load(open(r'./demandlib/business.pkl', 'rb'))),
    'industry': np.asarray(np.load(open(r'./demandlib/industry.pkl', 'rb')))
}

winter = np.asarray(np.load(open(r'./demandlib/winter.pkl', 'rb')))
summer = np.asarray(np.load(open(r'./demandlib/summer.pkl', 'rb')))

class StandardLoadProfile:

    def __init__(self, demandP, type='household', hourly=True):

        self.type = type
        self.demandP = demandP
        self.profile = profiles[type]

        self.winter = winter
        self.summer = summer

        self.hourly = hourly

    def run_model(self, d):

        doy = d.dayofyear
        dow = d.dayofweek

        f = self.demandP / 10 ** 6
        if self.type == 'household':
            f *= -0.000000000392 * doy ** 4 + 0.00000032 * doy ** 3 - 0.0000702 * doy ** 2 + 0.0021 * doy + 1.24

        demand = np.zeros(96)

        if doy in self.summer:
            if dow == 6 or doy in get_holidays(doy):
                demand = self.profile[:, 4] * f
            elif dow < 5:
                demand = self.profile[:, 5] * f
            elif dow == 5:
                demand = self.profile[:, 3] * f
        elif doy in self.winter:
            if dow == 6 or doy in get_holidays(doy):
                demand = self.profile[:, 0] * f
            elif dow < 5:
                demand = self.profile[:, 2] * f
            elif dow == 5:
                demand = self.profile[:, 3] * f
        else:
            if dow == 6 or doy in get_holidays(doy):
                demand = self.profile[:, 7] * f
            elif dow < 5:
                demand = self.profile[:, 8] * f
            elif dow == 5:
                demand = self.profile[:, 6] * f

        if self.hourly:
            return np.asarray([np.mean(demand[i:i + 3]) for i in range(0, 96, 4)], np.float).reshape((-1,))
        else:
            return demand