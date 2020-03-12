import requests

class restInferace:

    def __init__(self, host='149.201.88.150', port=5010):
        self.market_plattform = 'http://' + host + ':%s/' %port

    def login(self, name, typ):
        reserve = 'X'
        if typ == 'DEM':
            reserve = 'O'
        r = requests.post(self.market_plattform + 'login', json={'uuid': name, 'area': name.split('_')[1],
                                                                 'typ': name.split('_')[0], 'reserve': reserve})
        if r.ok: print('login complete: ' + str(r.json()))

    def logout(self, name):
        r = requests.post(self.market_plattform + 'logout', json={'uuid': name})
        if r.ok: print('logout complete: ' + str(r.json()))

    def sendBalancing(self, orders):
        r = requests.post(self.market_plattform + 'balancing', json=orders)
        if r.ok:
            return True
        else:
            return False

    def sendDayAhead(self,orders):
        r = requests.post(self.market_plattform + 'orders', json=orders)
        if r.ok:
            return True
        else:
            return False

    def sendActuals(self, orders):
        r = requests.post(self.market_plattform + 'actuals', json=orders)
        if r.ok:
            return True
        else:
            return False

