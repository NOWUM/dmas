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
        'plz': int(os.getenv('PLZ_CODE', 1)),
        'type': os.getenv('TYPE', 'CTL'),
        'mqtt_server': os.getenv('MQTT_HOST', '10.13.10.54'),
        'mqtt_exchange': os.getenv('MQTT_EXCHANGE', 'dMAS'),
        'simulation_server': os.getenv('SIMULATION_SOURCE', '10.13.10.54:5432'),
        'simulation_database': os.getenv('SIMULATION_DATABASE', 'dmas'),
        'structure_server': os.getenv('STRUCTURE_SERVER', '10.13.10.41:5432'),
        'structure_credential': os.getenv('STRUCTURE_CREDENTIAL', 'opendata:opendata'),
    }

    try:

        if init_dict['type'] == 'DEM':
            agent = DemAgent(**init_dict)
        elif init_dict['type'] == 'RES':
            agent = ResAgent(**init_dict)
        elif init_dict['type'] == 'PWP':
            agent = PwpAgent(**init_dict)
        elif init_dict['type'] == 'STR':
            init_dict.update({'type': 'STR'})
            agent = StrAgent(**init_dict)
        elif init_dict['type'] == 'NET':
            init_dict.update({'type': 'NET'})
            agent = NetAgent(**init_dict)
        elif init_dict['type'] == 'MRK':
            agent = MarketAgent(**init_dict)
        elif init_dict['type'] == 'WTR':
            agent = WtrAgent(**init_dict)
        elif init_dict['type'] == 'CTL':
            agent = CtlAgent(**init_dict)

        agent.run()
        #agent.optimize_day_ahead()

    except Exception as e:
        logging.exception(f'Error during Simulation {init_dict["type"]}_{init_dict["plz"]}')
