import logging
import os
from agents.dem_Agent import DemAgent
from agents.res_Agent import ResAgent
from agents.pwp_Agent import PwpAgent
from agents.str_Agent import StrAgent
from agents.net_Agent import NetAgent
from agents.mrk_Agent import MarketAgent
from agents.wtr_Agent import WtrAgent
from agents.ctl_Agent import CtlAgent


logging.basicConfig()

if __name__ == "__main__":

    init_dict = {
        'date': os.getenv('SIMULATION_START_DATE', '1995-01-01'),
        'plz': int(os.getenv('PLZ_CODE', 415)),
        'mqtt_exchange': os.getenv('MQTT_EXCHANGE', 'dMAS'),
        'agent_type': os.getenv('AGENT_TYPE', 'CTL'),
        'connect': bool(os.getenv('CONNECT', False)),
        'infrastructure_source': os.getenv('INFRASTRUCTURE_SOURCE', '10.13.10.41:5432'),
        'infrastructure_login': os.getenv('INFRASTRUCTURE_LOGIN', 'opendata:opendata')
    }

    try:

        if init_dict['agent_type'] == 'DEM':
            agent = DemAgent(**init_dict)
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
            agent = WtrAgent(**init_dict)
        elif init_dict['agent_type'] == 'CTL':
            agent = CtlAgent(**init_dict)

        agent.run()
        #agent.optimize_day_ahead()

    except Exception as e:
        logging.exception(f'Error during Simulation {init_dict["agent_type"]}_{init_dict["plz"]}')
