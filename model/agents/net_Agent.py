# third party modules
import time
import pandas as pd
import numpy as np

# model modules
from agents.basic_Agent import BasicAgent
from systems.transmission_system import TransmissionSystem

class NetAgent(BasicAgent):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        start_time = time.time()


        # TODO: Add nodes and edges
        data = {
            'nodes': self.infrastructure_interface.get_grid_nodes(),
            'edges': self.infrastructure_interface.get_grid_edges()
        }
        self.transmission_system = TransmissionSystem(**data)

        self.logger.info(f'setup of the agent completed in {time.time() - start_time:.2f} seconds')

    def handle_message(message):
        if 'calculate_power_flow' in message:
            self.calculate_power_flow()

    def calculate_power_flow(self):
        # TODO Query Demand and Generation per Area
        self.transmission_system.set_parameter(self.date)
        self.transmission_system.optimize()
