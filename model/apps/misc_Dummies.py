from datetime import date, datetime
from apps.misc_Holiday import getHolidays
import numpy as np
import pandas as pd

def getSeason(now):

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


def createSaisonDummy(start, end, hour=False):

    if end < (start+pd.DateOffset(days=366)):
        end_ = start+pd.DateOffset(days=366)
    else:
        end_ = end

    if hour:
        index = pd.date_range(start=start, end=end_)
        hourofday = [str(i.hour) for i in index]
        dayofweek = [i.day_name() for i in index]
        monthofyear = [i.month_name() for i in index]
        holidays = [getHolidays(year)[0] for year in np.unique([i.year for i in index])]
        holidays = [i for h in holidays for i in h]
        holidays = [int(i in holidays) for i in index]
        seasons = [getSeason(i) for i in index]
        df = pd.DataFrame(index=index,data={'hourofday':hourofday, 'dayofweek':dayofweek, 'monthofyear':monthofyear,'holidays':holidays,'seasons':seasons})
    else:
        index = pd.date_range(start=start, end=end_, freq='D')
        dayofweek = [i.day_name() for i in index]
        monthofyear = [i.month_name() for i in index]
        holidays = [getHolidays(year)[0] for year in np.unique([i.year for i in index])]
        holidays = [i for h in holidays for i in h]
        holidays = [int(i in holidays) for i in index]
        seasons = [getSeason(i) for i in index]
        df = pd.DataFrame(index=index, data={'dayofweek':dayofweek, 'monthofyear':monthofyear,'holidays':holidays,'seasons':seasons})

    df = pd.get_dummies(df)
    df = df.loc[df.index >= start]
    df = df.loc[df.index <= end]

    return df.to_numpy()