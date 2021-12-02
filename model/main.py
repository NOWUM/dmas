import logging
import os
# from agents.dem_Agent import DemAgent
from agents.res_Agent import ResAgent
from agents.pwp_Agent import PwpAgent
from agents.str_Agent import StrAgent
from agents.net_Agent import NetAgent
from agents.mrk_Agent import MarketAgent
from agents.wtr_Agent import WtrAgent

logging.basicConfig()

if __name__ == "__main__":

    init_dict = {
        'date': os.getenv('SIMULATION_START_DATE', '2018-01-01'),
        'plz': os.getenv('PLZ_CODE', 98),
        'mqtt_exchange': os.getenv('MQTT_EXCHANGE', 'dMAS'),
        'simulation_database': os.getenv('SIMULATIONS_DATABASE', 'dMAS'),
        'agent_type': os.getenv('AGENT_TYPE', 'STR'),
        'connect': os.getenv('CONNECT', False),
    }

    try:

        agent = None
        if init_dict['agent_type'] == 'DEM':
            # agent = DemAgent(**init_dict)
            pass
        elif init_dict['agent_type'] == 'RES':
            agent = ResAgent(**init_dict)
        elif init_dict['agent_type'] == 'PWP':
            agent = PwpAgent(**init_dict)
        elif init_dict['agent_type'] == 'STR':
            agent = StrAgent(**init_dict)
        elif init_dict['agent_type'] == 'NET':
            agent = NetAgent(**init_dict)
        elif init_dict['agent_type'] == 'MRK':
            agent = MarketAgent(**init_dict)
        elif init_dict['agent_type'] == 'WTR':
            init_dict.update({'weather_database': os.getenv('WEATHER_DATABASE', 'weather')})
            init_dict.update({'weather_host': os.getenv('WEATHER_HOST', 'weather')})
            agent = WtrAgent(**init_dict)

        # agent.run()

    except Exception as e:
        logging.exception(f'Error during Simulation {agent.name}')
