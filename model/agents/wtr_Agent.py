# third party modules
import time as tme
import argparse
import pandas as pd
import numpy as np

# model modules
from aggregation.portfolio_demand import DemandPortfolio
from agents.client_Agent import agent as basicAgent

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--plz', type=int, required=False, default=50, help='PLZ-Agent')
    return parser.parse_args()


class WtrAgent(basicAgent):

    def __init__(self):
        pass


