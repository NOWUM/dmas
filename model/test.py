from main import init_dict, type_mapping

# run docker-compose up -d simulationdb rabbitmq grafana
from systems.generation_wind import WindModel
if __name__ == '__main__':
    init_dict['type'] = 'RES'
    init_dict['area'] = 'DE11A'
    agent_class = type_mapping[init_dict['type']]
    res_agent = agent_class(**init_dict)
    res_agent.optimize_day_ahead()
    res_agent.portfolio_eeg.capacities
    res_agent.portfolio_mrk.capacities
    res_agent.portfolio_eeg.generation
    res_agent.portfolio_mrk.generation
    wp = res_agent.portfolio_mrk.energy_systems[0]
    for wp in res_agent.portfolio_mrk.energy_systems:
        if isinstance(wp, WindModel):
            wp.mc.run_model(wp.weather)
            assert all(wp.mc.power_output <= wp.mc.power_plant.nominal_power)


    init_dict['type'] = 'DEM'
    agent_class = type_mapping[init_dict['type']]
    dem_agent = agent_class(**init_dict)
    dem_agent.optimize_day_ahead()
    dem_agent.portfolio.capacities
    dem_agent.portfolio.demand
    dem_agent.portfolio.generation['total']
    t = dem_agent.portfolio.generation['total']
    d = dem_agent.portfolio.demand['power']
    assert all(t-d < 0)
    pv = dem_agent.portfolio.energy_systems[-2]
    pv.profile_generator.demandP/1e6 # ~300 GWh/year

    init_dict['type'] = 'PWP'
    agent_class = type_mapping[init_dict['type']]
    pwp_agent = agent_class(**init_dict)
    pwp_agent.optimize_day_ahead()
    pwp_agent.get_order_book()
    pwp_agent.portfolio.capacities