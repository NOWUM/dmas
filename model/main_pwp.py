import logging
import os
from agents.pwp_Agent import PwpAgent

if __name__ == "__main__":

    init_dict = {
        'date': os.getenv('SIMULATION_START_DATE', '2018-01-01'),
        'plz': os.getenv('PLZ_CODE', 52),
        'mqtt_exchange': os.getenv('MQTT_EXCHANGE', 'dMAS'),
        'simulation_database': os.getenv('SIMULATIONS_DATABASE', 'dMAS')
    }

    agent = PwpAgent(**init_dict)
    try:
        agent.run()
    except Exception as e:
        logging.exception(f'Error during Simulation {agent.name}')

