import numpy as np
from main import init_dict, type_mapping


def test_dem():
    init_dict['type'] = 'DEM'
    init_dict['area'] = 'DE13A'
    agent_class = type_mapping[init_dict['type']]
    agent = agent_class(**init_dict)
    agent.optimize_day_ahead()

    bid_orders = agent.portfolio.get_bid_orders()
    bid_volume = bid_orders['volume'].values
    demand = agent.portfolio.demand['power'] - agent.portfolio.generation['total']
    assert all(bid_volume == bid_volume)
    agent.post_day_ahead()
    demand_ = agent.portfolio.demand['power'] - agent.portfolio.generation['total']
    assert all(demand == demand_)

def test_res():
    init_dict['type'] = 'RES'
    init_dict['area'] = 'DE13A'
    agent_class = type_mapping[init_dict['type']]
    agent = agent_class(**init_dict)
    agent.optimize_day_ahead()

    ask_market = agent.portfolio_mrk.get_ask_orders(price=-0.001)
    ask_volume = ask_market['volume'].values
    generation = agent.portfolio_mrk.generation['total']
    assert all(ask_volume == generation)

    ask_eeg = agent.portfolio_eeg.get_ask_orders(price=-0.5)
    ask_volume = ask_eeg['volume'].values
    generation = agent.portfolio_eeg.generation['total']
    assert all(ask_volume == generation)
    # agent.post_day_ahead()

def test_pwp():
    init_dict['type'] = 'PWP'
    init_dict['area'] = 'DE13A'
    agent_class = type_mapping[init_dict['type']]
    agent = agent_class(**init_dict)
    agent.optimize_day_ahead()

    ask_orders = agent.portfolio.get_ask_orders()
    total_power = sum([system.generation['total'] for system in agent.portfolio.energy_systems])
    agent.post_day_ahead()

def test_str():
    init_dict['type'] = 'STR'
    init_dict['area'] = 'DE13A'
    agent_class = type_mapping[init_dict['type']]
    agent = agent_class(**init_dict)
    agent.optimize_day_ahead()
    ex_orders = agent.portfolio.get_exclusive_orders()
    ex_orders = ex_orders.reset_index()
    power = ex_orders.loc[ex_orders['block_id'] == 2, ['hour', 'name', 'volume']]
    power['volume'] *= 0.8
    agent.portfolio.optimize_post_market(power, power_prices=None)
    print(agent.portfolio.energy_systems[0].generation['storage'])
    print(power.loc[power['name'] == agent.portfolio.energy_systems[0].name, 'volume'].values)

if __name__ == '__main__':
    #test_dem()
    test_str()
    #test_pwp()
    #test_res()