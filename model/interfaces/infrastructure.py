from sqlalchemy import create_engine


class Infrastructure:

    def __init__(self, user='opendata', password='opendata', database='mastr', host='10.13.10.41', port=5432):

        self.engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

    def get_wind_turbines_in_area(self, area=50):
        query = ''

        pass

    def get_free_solar_systems_in_area(self, area=50):
        pass

    def get_biomass_systems_in_area(self, area=50):
        pass

    def get_run_river_systems_in_area(self, area=50):
        pass



if __name__ == "__main__":

    engine = create_engine(f'postgresql://opendata:opendata@10.13.10.41:5432/mastr')
    