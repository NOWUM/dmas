from sqlalchemy import create_engine
import pandas as pd
import numpy as np


ftime_pg = {'day': 'YYYY-MM-DD',
            'month': 'YYYY-MM-01',
            'year': 'YYYY-01-01',
            'hour': 'YYYY-MM-DD hh24:00:00',
            'minute': 'YYYY-MM-DD hh24:mi:00'}


# select column_name from information_schema.columns where table_name='turbineData'
class EntsoeInfrastructureInterface:

    def __init__(self, name, database_url):
        self.database_entsoe = create_engine(database_url,
                                             connect_args={"application_name": name})

        # self.geo_information = gpd.read_file(r'./interfaces/data/NUTS_EU.shp')
        # self.geo_information = self.geo_information.to_crs(4326)

    def get_energy_system_in_land(self, land):
        query = f'select index as "unitID", ' \
                f'production_type as "fuelType", ' \
                f'"installed_capacity_[mw]" as "maxPower" ' \
                f'from query_installed_generation_capacity_per_unit ' \
                f"where country = '{land}'"
        df = pd.read_sql(query, self.database_entsoe)
        df['maxPower'] *= 1e3  # [MW] -> [kW]
        for index, row in df.iterrows():
            pass

        return df

    def get_demand_in_land(self, land, begin, end, groupby='hour'):
        query = f"select to_char(index, '{ftime_pg[groupby]}') as time, avg(actual_load) as actual_load from query_load " \
                f"where country = '{land}' and index >= '{begin}' and index < '{end}' group by time order by time"
        df = pd.read_sql(query, self.database_entsoe, index_col='time', parse_dates=['time'])
        df['actual_load'] *= 1e3  # [MW] -> [kW]

        return df

    def get_generation_in_land(self, land, begin, end, groupby='hour'):

        query = f'''
SELECT to_char(index, '{ftime_pg[groupby]}') as time,
avg(biomass) as bio,
avg("hydro_water_reservoir"+"hydro_run-of-river_and_poundage") as water,
avg("wind_offshore"+"wind_onshore") as wind,
avg(solar) as solar,
avg(nuclear) as nuclear,
avg("fossil_brown_coal/lignite") as lignite,
avg(fossil_oil+fossil_hard_coal) as coal,
avg(fossil_gas+"fossil_coal-derived_gas") as gas,
avg(hydro_pumped_storage) as storage,
avg(geothermal+other+other_renewable+waste) as other
from query_generation
where country = '{land}' and index >= '{begin}' and index < '{end}'
group by time
order by time
'''
        df = pd.read_sql(query, self.database_entsoe, index_col='time', parse_dates=['time'])
        return df * 1e3

    def get_price_in_land(self, land, begin, end, groupby='hour'):
        query = f"""
select to_char(index, '{ftime_pg[groupby]}') as time, avg("0") as "da_price"
from query_day_ahead_prices
where country like '{land}%%' and index >= '{begin}' and index < '{end}'
group by time
"""
        df = pd.read_sql(query, self.database_entsoe, index_col='time', parse_dates=['time'])
        df['da_price']  # [€/MWh]
        return df

    def get_global_capacities(self, year):
        query = f"""
SELECT index, biomass as bio,
"hydro_water_reservoir"+"hydro_run-of-river_and_poundage" as water,
"wind_offshore"+"wind_onshore" as wind,
solar as solar,
nuclear as nuclear,
"fossil_brown_coal/lignite" as lignite,
fossil_oil+fossil_hard_coal as coal,
fossil_gas+"fossil_coal-derived_gas" as gas,
hydro_pumped_storage as storage,
geothermal+other+other_renewable+waste as other
FROM query_installed_generation_capacity WHERE country = 'DE' and index<'{year}-01-01'
ORDER BY 1 DESC
Limit 1
"""
        return pd.read_sql(query, self.database_entsoe)


if __name__ == '__main__':
    DB_URI = 'postgresql://readonly:readonly@10.13.10.41:5432/entsoe'
    entsoe = EntsoeInfrastructureInterface('INFRA', DB_URI)
    from datetime import timedelta
    begin = pd.to_datetime('2018-01-01')
    end = begin + timedelta(days=1)
    dem = entsoe.get_demand_in_land('DE', begin.date(), end.date())

    capacities = entsoe.get_global_capacities(2019)

    generation = entsoe.get_generation_in_land('DE', begin.date(), end.date())
    price = entsoe.get_price_in_land('DE', begin.date(), end.date())
