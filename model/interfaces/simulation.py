from sqlalchemy import create_engine


class SimulationInterface():

    def __init__(self, user='dMas', password='dMas', database='dMas'):
        self.engine = create_engine(f'postgresql://{user}:{password}@\'simulationdb\'/{database}')

    def get_demand(self, date):
        return []

    def get_power_price(self, date):
        return []
