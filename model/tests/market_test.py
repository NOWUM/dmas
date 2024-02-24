import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from systems.utils import visualize_orderbook
from systems.powerPlant import PowerPlant
from systems.storage_hydroPlant import Storage
from systems.market import DayAheadMarket
from aggregation.portfolio_powerPlant import PowerPlantPortfolio


def test_market():
    max_p = 1500
    min_p = max_p*0.2
    plant = {'unitID': 'x',
             'fuel': 'coal',
             'maxPower': max_p,  # kW
             'minPower': min_p,  # kW
             'eta': 0.4,  # Wirkungsgrad
             'P0': 0,
             'chi': 0.407 / 1e3,  # t CO2/kWh
             'stopTime': 12,  # hours
             'runTime': 6,  # hours
             'gradP': min_p,  # kW/h
             'gradM': min_p,  # kW/h
             'on': 2,  # running since
             'off': 0,
             'startCost': 1e3  # €/Start
             }
    steps = [-100, 0, 100]

    portfolio = PowerPlantPortfolio()
    portfolio.add_energy_system(plant)

    power_price = 50 * np.ones(48)
    power_price[18:24] = 0
    power_price[24:] = 20
    co = np.ones(48) * 23.8  # * np.random.uniform(0.95, 1.05, 48)     # -- Emission Price     [€/t]
    gas = np.ones(48) * 0.03  # * np.random.uniform(0.95, 1.05, 48)    # -- Gas Price          [€/kWh]
    lignite = np.ones(48) * 0.015  # * np.random.uniform(0.95, 1.05)   # -- Lignite Price      [€/kWh]
    coal = np.ones(48) * 0.02  # * np.random.uniform(0.95, 1.05)       # -- Hard Coal Price    [€/kWh]
    nuc = np.ones(48) * 0.01  # * np.random.uniform(0.95, 1.05)        # -- nuclear Price      [€/kWh]

    prices = dict(power=power_price, gas=gas, co=co, lignite=lignite, coal=coal, nuc=nuc)
    prices = pd.DataFrame(data=prices, index=pd.date_range(start='2018-01-01', freq='h', periods=48))

    pwp = PowerPlant(T=24, steps=steps, **plant)
    power = pwp.optimize(date=pd.Timestamp(2018, 1, 1), weather=None,
                         prices=prices)

    portfolio.optimize(date=pd.Timestamp(2018, 1, 1), prices=prices, weather=pd.DataFrame())

    o_book = portfolio.get_ask_orders()

    min_bid = -500/1e3
    o_book.loc[o_book['price'] < min_bid, 'price'] = min_bid
    # order book for first day without market
    # visualize_orderbook(o_book)

    demand = np.ones(24) * 4000
    demand[6:9] = 5000
    demand[16:20] = 5000

    def demand_order_book(demand):
        order_book = {}
        for t in range(24):
            if -demand[t] < 0:
                order_book[t] = dict(type='demand',
                                     hour=t,
                                     block_id=t,
                                     name='DEM',
                                     price=3,  # €/kWh
                                     volume=-demand[t])

        demand_order = pd.DataFrame.from_dict(order_book, orient='index')
        demand_order = demand_order.set_index(['block_id', 'hour', 'name'])
        return demand_order

    demand_order = demand_order_book(demand)

    storage = {"eta_plus": 0.8, "eta_minus": 0.87, "fuel": "water", "PPlus_max": 1000,
               "PMinus_max": 1000, "V0": 4000, "VMin": 0, "VMax": 8000}

    sys = Storage(T=24, unitID='x', **storage)
    str_prices = prices.copy()
    str_prices['power'] /= 1000
    sys.optimize(pd.Timestamp(2018, 1, 1), prices=str_prices, weather=pd.DataFrame())
    ex_orders = sys.get_exclusive_orders()

    my_market = DayAheadMarket()

    hourly_bid = {}
    for key, value in demand_order.to_dict(orient='index').items():
        hourly_bid[key] = (value['price'], value['volume'])

    # hourly_ask = {}
    # for key, value in res_order.to_dict(orient='index').items():
    #     hourly_ask[key] = (value['price'], value['volume'])

    linked_orders = {}
    for key, value in o_book.to_dict(orient='index').items():
        linked_orders[key] = (value['price'], value['volume'], value['link'])

    exclusive_orders = {}
    for key, value in ex_orders.to_dict(orient='index').items():
        exclusive_orders.update({key: (value['price'], value['volume'])})

    # # print(linked_orders)
    my_market.set_parameter({}, hourly_bid, linked_orders, exclusive_orders)
    # optimize and unpack
    result = my_market.optimize()
    assert result, "no result"
    prices_market, used_ask_orders, used_linked_orders, used_exclusive_orders, used_bid_orders, merit_order = result
    my_market.model.use_linked_order.pprint()

    committed_power = used_linked_orders.groupby('hour').sum()['volume']
    # assert committed_power[23] == 300, 'pwp bids negative values'

    # plot committed power of the pwp for results of first day
    comm = np.zeros(24)
    comm[committed_power.index] = committed_power
    committed_power.plot()
    (-demand_order['volume']).plot()
    #(res_order['volume']).plot()

    power = pwp.optimize_post_market(comm, prices_market['price'].values)
    ############### second day ##############

    power = pwp.optimize(date=pd.Timestamp(2018, 1, 2), weather=None,
                         prices=prices)
    o_book = pwp.get_ask_orders()
    visualize_orderbook(o_book)

    hourly_bid = {}
    for key, value in demand_order.to_dict(orient='index').items():
        hourly_bid[key] = (value['price'], value['volume'])

    linked_orders = {}
    for key, value in o_book.to_dict(orient='index').items():
        linked_orders[key] = (value['price'], value['volume'], value['link'])

    my_market.set_parameter({}, hourly_bid, linked_orders, {})
    prices_market, used_ask_orders, used_linked_orders, used_exclusive_orders, used_bid_orders, merit_order = my_market.optimize()

    committed_power = used_linked_orders.groupby('hour').sum()['volume']
    comm = np.zeros(24)
    comm[committed_power.index] = committed_power
    committed_power.plot()
    power = pwp.optimize_post_market(comm)
    my_market.model.use_linked_order.pprint()