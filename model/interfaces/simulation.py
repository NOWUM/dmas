from sqlalchemy import create_engine

class SimulationInterface():

    def __init__(self, user='dMas', password='dMas', database='dMas'):
        self.engine = create_engine(f'postgresql://{user}:{password}@\'simulationdb\'/{database}')

