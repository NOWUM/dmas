from datetime import date, datetime, timedelta
import numpy as np
import pandas as pd


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
    holidays.append(easter - timedelta(days=2))  # -- Karfreitag
    holidays.append(easter + timedelta(days=1))  # -- Ostermontag
    holidays.append(easter + timedelta(days=39))  # -- Christihimmelfahrt
    holidays.append(easter + timedelta(days=49))  # -- Pfingstsonntag
    holidays.append(easter + timedelta(days=50))  # -- Pfingstmontag
    holidays.append(easter + timedelta(days=60))  # -- Fronleichnam
    holidays.append(date(year, 12, 24))  # -- 1. Weihnachtstag
    holidays.append(date(year, 12, 25))  # -- 1. Weihnachtstag
    holidays.append(date(year, 12, 26))  # -- 2. Weihnachtstag
    holidays.append(date(year, 12, 31))  # -- 2. Weihnachtstag
    holidays.append(date(year, 1, 1))  # -- Neujahr
    holidays.append(date(year, 5, 1))  # -- 1. Mai
    holidays.append(date(year, 10, 3))  # -- Tag der deutschen Einheit
    holidays.append(date(year, 10, 31))  # -- Reformationstag

    return np.asarray([h.timetuple().tm_yday for h in holidays])


def get_season(now):

    seasons = [('winter', (date(now.year, 1, 1), date(now.year, 3, 20))),
               ('spring', (date(now.year, 3, 21), date(now.year, 6, 20))),
               ('summer', (date(now.year, 6, 21), date(now.year, 9, 22))),
               ('autumn', (date(now.year, 9, 23), date(now.year, 12, 20))),
               ('winter', (date(now.year, 12, 21), date(now.year, 12, 31)))]

    if isinstance(now, datetime):
        now = now.date()
    now = now.replace(year=now.year)
    return next(season for season, (start, end) in seasons
                if start <= now <= end)

def create_dummies(d):

    def get_dummies(x):
        index = pd.date_range(start=x, periods=24, freq='h')
        hours = [str(i.hour) for i in index]
        days = [i.day_name() for i in index]
        months = [i.month_name() for i in index]
        holidays = [i in get_holidays(x.year) for i in index]
        seasons = [get_season(i) for i in index]
        df = pd.DataFrame(index=index, data={'hourofday':hours, 'dayofweek':days, 'monthofyear':months,'holidays':holidays,'seasons':seasons})
        return df

    dummies = pd.concat([get_dummies(x) for x in pd.date_range(start=d, periods=366)])
    dummies = pd.get_dummies(dummies).replace(False, 0)

    return dummies.to_numpy()[:24,:]
