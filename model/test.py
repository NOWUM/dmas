import pandas as pd

from main import init_dict, type_mapping
import numpy as np
# run docker compose up -d simulationdb rabbitmq grafana


if __name__ == '__main__':

    test_agent = 'PWP'

    init_dict['type'] = test_agent
    init_dict['area'] = 'DE111'
    agent_class = type_mapping[init_dict['type']]
    agent = agent_class(**init_dict)
    agent.date = pd.Timestamp(2018,1,1)
    agent.optimize_day_ahead()

    if test_agent == 'DEM':
        bid_orders = agent.portfolio.get_bid_orders()
        bid_volume = bid_orders['volume'].values
        demand = agent.portfolio.demand['power'] - agent.portfolio.generation['total']
        assert all(bid_volume == bid_volume)
        agent.post_day_ahead()
        demand_ = agent.portfolio.demand['power'] - agent.portfolio.generation['total']
        assert all(demand == demand_)
    elif test_agent == 'RES':
        ask_market = agent.portfolio_mrk.get_ask_orders(price=-0.001)
        ask_volume = ask_market['volume'].values
        generation = agent.portfolio_mrk.generation['total']
        assert all(ask_volume == generation)

        ask_eeg = agent.portfolio_eeg.get_ask_orders(price=-0.5)
        ask_volume = ask_eeg['volume'].values
        generation = agent.portfolio_eeg.generation['total']
        assert all(ask_volume == generation)
        # agent.post_day_ahead()
    elif test_agent == 'PWP':
        ask_orders = agent.portfolio.get_ask_orders()
        total_power = sum([system.generation['total'] for system in agent.portfolio.energy_systems])
        agent.post_day_ahead()
    elif test_agent == 'STR':
        ex_orders = agent.portfolio.get_exclusive_orders()
        ex_orders = ex_orders.reset_index()
        power = ex_orders.loc[ex_orders['block_id'] == 2, ['hour', 'name', 'volume']]
        power['volume'] *= 0.8
        agent.portfolio.optimize_post_market(power, power_prices=None)
        print(agent.portfolio.energy_systems[0].generation['storage'])
        print(power.loc[power['name'] == agent.portfolio.energy_systems[0].name, 'volume'].values)
    # wp = res_agent.portfolio_mrk.energy_systems[0]
    # for wp in res_agent.portfolio_mrk.energy_systems:
    #     if isinstance(wp, WindModel):
    #         wp.mc.run_model(wp.weather)
    #         assert all(wp.mc.power_output <= wp.mc.power_plant.nominal_power)
    #
    #
    # init_dict['type'] = 'DEM'
    # agent_class = type_mapping[init_dict['type']]
    # dem_agent = agent_class(**init_dict)
    # dem_agent.optimize_day_ahead()
    # dem_agent.portfolio.capacities
    # dem_agent.portfolio.demand
    # dem_agent.portfolio.generation['total']
    # t = dem_agent.portfolio.generation['total']
    # d = dem_agent.portfolio.demand['power']
    # assert all(t-d < 0)
    # pv = dem_agent.portfolio.energy_systems[-2]
    # pv.profile_generator.demandP/1e6 # ~300 GWh/year
    #
    # init_dict['type'] = 'PWP'
    # agent_class = type_mapping[init_dict['type']]
    # pwp_agent = agent_class(**init_dict)
    # pwp_agent.optimize_day_ahead()
    # pwp_orderbook = pwp_agent.get_ask_orders()
    # pwp_agent.portfolio.capacities
    #
    # pwp = pwp_agent.portfolio.energy_systems[0]
    # pwp.start_cost # €/kWh
    # plant = pwp.power_plant
    # prices = pwp.prices
    # clean_spread = (1/plant['eta'] * (prices[plant['fuel']].mean() + plant['chi'] * prices['co'].mean()))
    #
    # print(pwp.get_clean_spread()) # €/kWh cost
    # print(pwp.power_plant['maxPower']*pwp.get_clean_spread())
    #
    # # Gesamterlöse
    # for k,v in pwp.optimization_results.items(): print(k,v['obj'])
    # # Fahrpläne
    # for k,v in pwp.optimization_results.items(): print(k,v['power'])
    # high_prices = list(pwp.optimization_results.values())[-1]
    # assert all(high_prices['power']) > 0, 'pwp must be running'
    # assert high_prices['obj'] > 0 # günstig sollte sich immer lohnen
    #
    # init_dict['type'] = 'MRK'
    # agent_class = type_mapping[init_dict['type']]
    # mrk_agent = agent_class(**init_dict)
    # mrk = mrk_agent.market
    #
    # hourly_orders = mrk_agent.simulation_interface.get_hourly_orders()
    #
    # ask = hourly_orders.loc[hourly_orders['type'] == 'generation']
    # all(ask['volume'] > 0)
    # bid = hourly_orders.loc[hourly_orders['type'] == 'demand']
    # all(bid['volume'] < 0)
    # asks = ask['volume'].groupby('hour').sum()
    # bids = bid['volume'].groupby('hour').sum()
    # diff = asks + bids
    # # if diff[t] < 0 - renewables are not enough and pwp is needed
    #
    # linked = mrk_agent.simulation_interface.get_linked_orders()
    # exclusive_orders= mrk_agent.simulation_interface.get_exclusive_orders()