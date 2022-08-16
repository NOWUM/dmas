#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
from systems.powerPlant import PowerPlant

# @pytest.fixture(scope='module')

def is_eq(a,b):
    return all(abs(a - b) < 1e-10)

def create_pwp(max_p=1500):
    min_p = max_p * .4
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

    power_price = 50 * np.ones(48)
    power_price[18:24] = 0
    power_price[24:] = 20
    co = np.ones(48) * 23.8  # -- Emission Price     [€/t]
    gas = np.ones(48) * 0.03  # -- Gas Price          [€/kWh]
    lignite = np.ones(48) * 0.015  # -- Lignite Price      [€/kWh]
    coal = np.ones(48) * 0.02     # -- Hard Coal Price    [€/kWh]
    nuc = np.ones(48) * 0.01      # -- nuclear Price      [€/kWh]

    prices = dict(power=power_price, gas=gas, co=co,
                  lignite=lignite, coal=coal, nuc=nuc)
    prices = pd.DataFrame(data=prices, index=pd.date_range(
        start='2018-01-01', freq='h', periods=48))
    return plant.copy(), prices.copy()


def test_pwp():
    max_p = 1500
    plant, prices = create_pwp(max_p)
    steps = (-10, 0, 10, 1e6)

    pwp = PowerPlant(T=24, steps=steps, **plant)
    result = pwp.optimize('2018-01-01', None, prices)
    assert max(pwp.power) == max_p
    committed = pwp.optimize_post_market(result)
    test_committed = [
        600., 1200., 1500., 1500., 1500., 1500., 1500., 1500., 1500.,
        1500., 1500., 1500., 1500., 1500., 1500., 1500., 1500., 1500.,
        900.,    0.,    0.,    0.,    0.,    0.]
    assert is_eq(test_committed, committed)
    assert is_eq(committed, pwp.power)
    assert len(committed) == 24

def test_pwp_run_through():
    max_p = 1500
    plant, prices = create_pwp(max_p)

    plant['on'] = 24
    plant['P0'] = max_p
    prices['power'] += 10
    steps = (-10, 0, 10, 1e6)

    pwp = PowerPlant(T=24, steps=steps, **plant)
    result = pwp.optimize('2018-01-01', None, prices)
    assert is_eq(pwp.power, max_p)
    committed = pwp.optimize_post_market(result)
    test_committed = np.ones(24)*1500
    assert is_eq(test_committed, committed)
    assert is_eq(committed, pwp.power)
    assert len(committed) == 24

    result = pwp.optimize('2018-01-02', None, prices)
    assert is_eq(pwp.power, max_p)
    committed = pwp.optimize_post_market(result)
    test_committed = np.ones(24)*1500
    assert is_eq(test_committed, committed)
    assert is_eq(committed, pwp.power)
    assert len(committed) == 24


def test_minimize_diff():
    plant, prices = create_pwp(300)
    steps = (-100, 0, 100)
    pwp = PowerPlant(T=24, steps=steps, **plant)
    power = pwp.optimize(pd.Timestamp(2018, 1, 1), None, prices)
    expected = np.array([120., 240., 300., 300., 300., 300., 300., 
       300., 300., 300., 300.,
       300., 300., 300., 300., 300., 300., 300., 180.,   0.,   0.,   0.,
         0.,   0.])
    assert is_eq(power, expected)
    # power plant has to minimize loss, when market did something weird
    p = power.copy()
    p[4:10] = 0
    power_day2 = pwp.optimize_post_market(committed_power=p)
    # run minlast in relevant hours
    expected_opt_result = np.array([120., 240., 300., 300., 180., 120., 120., 120., 120., 180., 300.,
       300., 300., 300., 300., 300., 300., 300., 180.,   0.,   0.,   0.,
         0.,   0.])
    assert is_eq(power_day2, expected_opt_result)


def test_run_twice():
    plant, prices = create_pwp()
    steps = (-10, 0, 10)

    pwp = PowerPlant(T=24, steps=steps, **plant)
    result = pwp.optimize('2018-01-01', None, prices)
    result2 = pwp.optimize('2018-01-01', None, prices)
    assert is_eq(result, result2)
    committed = pwp.optimize_post_market(result)
    committed2 = pwp.optimize_post_market(result)
    assert is_eq(committed2, committed)