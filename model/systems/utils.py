from systems.powerPlant import PowerPlant
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap


def visualize_orderbook(order_book):
    tab20_cmap = plt.get_cmap("tab20c")
    ob = order_book.reset_index(level=[0, 2])
    idx = np.arange(24)
    max_block_count = max(ob['block_id'])

    y_past = np.zeros(24)
    for i, df_grouped in ob.groupby('block_id'):
        my_cmap_raw = np.array(tab20_cmap.colors) * i / max_block_count
        my_cmap = ListedColormap(my_cmap_raw)

        for j, o in df_grouped.groupby('link'):
            x = idx  # o.index
            ys = np.zeros(24)
            ys[o.index] = o['volume']
            if len(list(ys)) > 0 and (ys > 0).any():
                plt.bar(x, ys, bottom=y_past, color=my_cmap.colors[(j + 1) % 20])
            y_past += ys
    plt.title('Orderbook')
    plt.xlabel('hour')
    plt.ylabel('kW')
    plt.show()


def test_half_power(plant, prices):
    steps = (-100, 0, 100)
    pwp = PowerPlant(T=24, steps=steps, **plant)
    power = pwp.optimize(pd.Timestamp(2018, 1, 1), None, prices)
    o_book = pwp.get_ask_orders()
    # running since 1
    visualize_orderbook(o_book)

    # clean_spread = (1/plant['eta'] * (prices[plant['fuel']].mean() + plant['chi'] * prices['co'].mean()))
    print(f'{pwp.get_clean_spread()} €/kWh cost')
    print(f"{pwp.generation_system['maxPower'] * pwp.get_clean_spread()} €/h full operation")

    # assume market only gives you halve of your offers
    pwp.optimize_post_market(committed_power=power / 2)

    # running since 1
    visualize_orderbook(pwp.get_ask_orders())

    assert all((power / 2 - pwp.power) < 1e-10)  # smaller than threshold
    assert pwp.generation_system['on'] == 24
    assert pwp.generation_system['off'] == 0


def test_ramp_down(plant, prices):
    steps = (-100, 0, 100)
    pwp = PowerPlant(T=24, steps=steps, **plant)
    power = pwp.optimize(pd.Timestamp(2018, 1, 1), None, prices)

    visualize_orderbook(pwp.get_ask_orders())

    # power plant should ramp down correctly
    pwp.optimize_post_market(committed_power=power * 0)
    visualize_orderbook(pwp.get_ask_orders())

    assert pwp.generation_system['on'] == 0
    assert pwp.generation_system['off'] == 19

    power_day2 = pwp.optimize(pd.Timestamp(2018, 1, 2), None, prices)

    visualize_orderbook(pwp.get_ask_orders())

    # another day off - this time a full day
    pwp.optimize_post_market(committed_power=power * 0)
    visualize_orderbook(pwp.get_ask_orders())

    assert pwp.generation_system['on'] == 0
    assert pwp.generation_system['off'] == 24

    # for k,v in pwp.optimization_results.items(): print(k, v['power'])
    # for k,v in pwp.optimization_results.items(): print(k, v['obj'])
    # actual schedule corresponds to the market result


def test_minimize_diff(plant, prices):
    steps = (-100, 0, 100)
    pwp = PowerPlant(T=24, steps=steps, **plant)
    power = pwp.optimize(pd.Timestamp(2018, 1, 1), None, prices)

    visualize_orderbook(pwp.get_ask_orders())

    # power plant has to minimize loss, when market did something weird
    p = power.copy()
    p[4:10] = 0
    power_day2 = pwp.optimize_post_market(committed_power=p)
    # visualize_orderbook(pwp.get_ask_orders())

    return pwp


def test_up_down(plant, prices):
    steps = (-100, 0, 100)
    pwp = PowerPlant(T=24, steps=steps, **plant)
    power = pwp.optimize(pd.Timestamp(2018, 1, 1), None, prices)

    visualize_orderbook(pwp.get_ask_orders())

    # power plant has to minimize loss, when market did something weird
    p = power.copy()
    p[::2] = 0
    power_day2 = pwp.optimize_post_market(committed_power=p)
    visualize_orderbook(pwp.get_ask_orders())
    return pwp


if __name__ == "__main__":

    plant = {'unitID': 'x',
             'fuel': 'lignite',
             'maxPower': 300,  # kW
             'minPower': 100,  # kW
             'eta': 0.4,  # Wirkungsgrad
             'P0': 120,
             'chi': 0.407 / 1e3,  # t CO2/kWh
             'stopTime': 12,  # hours
             'runTime': 6,  # hours
             'gradP': 300,  # kW/h
             'gradM': 300,  # kW/h
             'on': 1,  # running since
             'off': 0,
             'startCost': 1e3  # €/Start
             }

    power_price = np.ones(48)  # * np.random.uniform(0.95, 1.05, 48) # €/kWh
    co = np.ones(48) * 23.8  # * np.random.uniform(0.95, 1.05, 48)     # -- Emission Price     [€/t]
    gas = np.ones(48) * 0.03  # * np.random.uniform(0.95, 1.05, 48)    # -- Gas Price          [€/kWh]
    lignite = np.ones(48) * 0.015  # * np.random.uniform(0.95, 1.05)   # -- Lignite Price      [€/kWh]
    coal = np.ones(48) * 0.02  # * np.random.uniform(0.95, 1.05)       # -- Hard Coal Price    [€/kWh]
    nuc = np.ones(48) * 0.01  # * np.random.uniform(0.95, 1.05)        # -- nuclear Price      [€/kWh]

    prices = dict(power=power_price, gas=gas, co=co, lignite=lignite, coal=coal, nuc=nuc)
    prices = pd.DataFrame(data=prices, index=pd.date_range(start='2018-01-01', freq='h', periods=48))

    test_half_power(plant, prices)

    test_ramp_down(plant, prices)

    plant['maxPower'] = 700  # kW
    test_ramp_down(plant, prices)

    plant['minPower'] = 10  # kW
    test_ramp_down(plant, prices)
    plant['maxPower'] = 600  # kW
    test_half_power(plant, prices)

    # test minimize difference
    plant['minPower'] = 10  # kW
    plant['maxPower'] = 600  # kW
    pwp = test_minimize_diff(plant, prices)
    # powerplant runs with minPower
    # currently no evaluation of start cost, if shutdown is possible
    assert pwp.generation_system['on'] == 24
    assert pwp.generation_system['off'] == 0

    plant['minPower'] = 10  # kW
    plant['maxPower'] = 600  # kW
    plant['stopTime'] = 1  # hours
    plant['runTime'] = 1  # hours
    # shut down if possible
    pwp = test_minimize_diff(plant, prices)

    plant['off'] = 3
    plant['on'] = 0
    plant['stopTime'] = 10
    pwp = test_up_down(plant, prices)