import numpy as np
from numba import jitclass
from numba import int32, int64, float32

spec = [    ('year', int32), ('startYear', int32), ('typ', int32),('refSLP', float32[:,:]), ('holiday', int64[:]),
            ('x_4',  float32), ('x_3',  float32), ('x_2',  float32), ('x_1',  float32), ('x_0',  float32),
            ('winter', int64[:]),('sommer', int64[:])]

@jitclass(spec)
class slpGen:

    def __init__(self, year=2019, typ=0,
                 refSLP=np.asarray(np.load(open(r'./data/Ref_H0.array','rb')), np.float32),
                 holidays=np.asarray(np.load(open(r'./data/Ref_Holiday.array','rb')), np.int64),
                 winter=np.asarray(np.load(open(r'./data/Time_Winter.array','rb')), np.int64),
                 sommer=np.asarray(np.load(open(r'./data/Time_Summer.array','rb')), np.int64)):

        self.startYear = 2005
        self.year = year
        self.holiday = holidays[self.year-self.startYear,:]

        self.x_4 = -0.000000000392
        self.x_3 = 0.00000032
        self.x_2 = -0.0000702
        self.x_1 = 0.0021
        self.x_0 = 1.24

        self.winter = winter
        self.sommer = sommer

        self.typ = typ
        self.refSLP = refSLP

    def get_profile(self, doy=200, dow=5, demand=1000):

        if self.typ == 0:
            f = 1
            #f = self.x_4 * doy ** 4 + self.x_3 * doy ** 3 + self.x_2 * doy ** 2 + self.x_1 * doy + self.x_0
        else:
            f = 1
        f *= demand/10**6

        for i in self.sommer:
            if i == doy:
                if dow < 5:
                    for h in self.holiday:
                        if h == doy:
                            return self.refSLP[:, 4] * f
                    return self.refSLP[:, 5] * f
                elif dow == 5:
                    for h in self.holiday:
                        if h == doy:
                            return self.refSLP[:, 4] * f
                    return self.refSLP[:, 3] * f
                return self.refSLP[:, 4] * f

        for i in self.winter:
            if i == doy:
                if dow < 5:
                    for h in self.holiday:
                        if h == doy:
                            return self.refSLP[:, 1] * f
                    return self.refSLP[:, 2] * f
                elif dow == 5:
                    for h in self.holiday:
                        if h == doy:
                            return self.refSLP[:, 1] * f
                    return self.refSLP[:, 0] * f
                return self.refSLP[:, 1] * f

        if dow < 5:
            for h in self.holiday:
                if h == doy:
                    return self.refSLP[:, 7] * f
            return self.refSLP[:, 8] * f
        elif dow == 5:
            for h in self.holiday:
                if h == doy:
                    return self.refSLP[:, 7] * f
            return self.refSLP[:, 6] * f
        return self.refSLP[:, 7] * f


if __name__ == '__main__':

    import pandas as pd

    num = 2019
    year = pd.date_range(start='01.01.' + str(num), end='31.12.' + str(num), freq='d')
    h0Gen = slpGen(typ=0)
    profile = []

    for day in year:
        profile.append(h0Gen.get_profile(day.dayofyear, day.dayofweek, 12000))
