import logging
import os
from agents.dem_Agent import DemAgent
from agents.res_Agent import ResAgent
from agents.pwp_Agent import PwpAgent
from agents.str_Agent import StrAgent
from agents.net_Agent import NetAgent
from agents.mrk_Agent import MarketAgent
from agents.ctl_Agent import CtlAgent
from agents.deo_Agent import DemEntsoeAgent

logging.basicConfig()

weather_server = os.getenv('WEATHER_SERVER', '10.13.10.41:5432')
weather_credential = os.getenv('WEATHER_CREDENTIAL', 'readonly:readonly')
weather_database = os.getenv('WEATHER_DATABASE', 'weather')
entsoe_database = os.getenv('ENTSOE_DATABASE', 'entsoe')
weather_database_uri = f'postgresql://{weather_credential}@{weather_server}/{weather_database}'
entsoe_database_uri = f'postgresql://{weather_credential}@{weather_server}/{entsoe_database}'
real_prices = os.getenv('REAL_PRICES', 'False').lower() == 'true'

init_dict = {
    'date': os.getenv('SIMULATION_START_DATE', '1995-01-01'),
    'area': os.getenv('AREA_CODE', 'DEA2D'),
    'type': os.getenv('TYPE', 'CTL'),
    # Websocket default parameter
    'ws_host': os.getenv('WS_HOST', 'localhost'),
    'ws_port': int(os.getenv('WS_PORT', 4000)),
    # simulation data server default parameter
    'simulation_server': os.getenv('SIMULATION_SOURCE', 'localhost:5432'),
    'simulation_credential': os.getenv('SIMULATION_DATABASE', 'dMAS:dMAS'),
    'simulation_database': os.getenv('SIMULATION_DATABASE', 'dmas'),
    # structure data server default parameter
    'structure_server': os.getenv('STRUCTURE_SERVER', '10.13.10.41:5432'),
    'structure_credential': os.getenv('STRUCTURE_CREDENTIAL', 'readonly:readonly'),
    # weather data server default parameter
    'weather_database_uri': weather_database_uri,
    'entsoe_database_uri': entsoe_database_uri,
    'real_prices': real_prices,
}

type_mapping = {
    'DEM': DemAgent,
    'RES': ResAgent,
    'PWP': PwpAgent,
    'STR': StrAgent,
    'NET': NetAgent,
    'MRK': MarketAgent,
    'CTL': CtlAgent,
    'DEO': DemEntsoeAgent,
}

if __name__ == "__main__":

    try:
        agent_class = type_mapping[init_dict['type']]
        agent = agent_class(**init_dict)

        agent.run()
        # agent.optimize_day_ahead()

    except Exception:
        logging.exception(f'Error during Simulation {init_dict["type"]}_{init_dict["area"]}')
