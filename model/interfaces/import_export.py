from sqlalchemy import create_engine
import pandas as pd
import geopandas as gpd
import numpy as np
import re


# select column_name from information_schema.columns where table_name='turbineData'
class InfrastructureInterface:

    def __init__(self, name, structure_data_server, structure_data_credential,
                 structure_databases='entsoe'):
        self.database_entsoe = create_engine(f'postgresql://{structure_data_credential}@{structure_data_server}/{structure_databases[0]}',
                                            connect_args={"application_name": name})

        self.geo_information = gpd.read_file(r'./interfaces/data/NUTS_EU.shp')
        self.geo_information = self.geo_information.to_crs(4326)

    def get_energy_system_in_land(self, land):
        query = f'select index as "unitID", ' \
                f'production_type as "fuelType", ' \
                f'"installed_capacity_[mw]" as "maxPower" ' \
                f'from query_installed_generation_capacity_per_unit ' \
                f'where country = {land}'
        df = pd.read_sql(self.database_entsoe, query)
        df['maxPower'] /= 1e3 # [MW] -> [kW]
        for index, row in df.iterrows():
            pass

        return df


