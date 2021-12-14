from sqlalchemy import create_engine
import pandas as pd


class SimulationInterface():

    def __init__(self, user='dMas', password='dMas', database='dMas'):
        self.engine = create_engine(f'postgresql://{user}:{password}@\'simulationdb\'/{database}')


    def get_orders(self):
        query = "Select * from orders"
        df = pd.read_sql(query, self.engine)
        return df


if __name__ == "__main__":
    engine = create_engine(f'postgresql://dMAS:dMAS@localhost/dMAS')
    query = "Select * from orders"
    df = pd.read_sql(query, engine)
