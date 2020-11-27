from collections import deque
import numpy as np

class WeekPriceList:
    def __init__(self, defaultDay: np.array(24)):
        self.prc_week: deque[np.array(24), 7] = deque(
            [defaultDay, defaultDay, defaultDay, defaultDay, defaultDay, defaultDay, defaultDay], 7)
        self.prc_today: np.array(24) = defaultDay
        # 2 control new data
        self.has_new_day = False
        self.set_new_day = True

    def get_price_yesterday(self):
        return self.prc_week[0]

    def get_price_week_before(self):
        return self.prc_week[-1]

    def get_price_x_days_before(self, x=0):
        return self.prc_week[x-1]

    def remember_price(self, prcToday: np.array(24)):
        if self.has_new_day:
            print("There is remembered data, which is not inserted into in week's deque... ")
        self.prc_today = prcToday
        self.has_new_day = True
        self.set_new_day = False

    def put_price(self):
        if self.set_new_day:
            print("There is NO new data, but the (old) remembered day is inserted into week's deque, "
                  "probably the second time ...")
        self.prc_week.appendleft(self.prc_today)
        self.has_new_day = False
        self.set_new_day = True

if __name__ == "__main__":
    pass