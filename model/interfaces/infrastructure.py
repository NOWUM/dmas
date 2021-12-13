from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import re
import geojson

# select column_name from information_schema.columns where table_name='turbineData'
class InfrastructureInterface:

    def __init__(self, infrastructure_source, infrastructure_login):
        # TODO: change connection args
        self.database_mastr = create_engine(f'postgresql://{infrastructure_login}@{infrastructure_source}/mastr',
                                            connect_args={"application_name": "myapp"})
        self.database_enet = create_engine(f'postgresql://{infrastructure_login}@{infrastructure_source}/enet',
                                           connect_args={"application_name": "myapp"})
        self.database_wind = create_engine(f'postgresql://{infrastructure_login}@{infrastructure_source}/windmodel',
                                           connect_args={"application_name": "myapp"})
        with open(r'./interfaces/data/germany_shape.geojson') as f:
            self.germany_shapes = geojson.load(f)
        with open(r'./interfaces/data/germany_centroid.geojson') as f:
            self.germany_centroid = geojson.load(f)
        self.centers = {int(element['properties']['plz']): tuple(element['geometry']['coordinates'])
                        for element in self.germany_centroid['features']}

        self.wind_codes = {
            'on_shore': 888,
            'off_shore': 889
        }

        #turbine_typs = pd.read_csv(r'./interfaces/data/technical_parameter_wind.csv')
        #self.pattern_wind = '(?:'
        #for typ in turbine_typs['turbine_type'].to_numpy():
        #    self.pattern_wind += str(typ).split('/')[0].replace('-', '') + '|'
        #self.pattern_wind += ')'
        #self.wind_manufacturer = pd.read_csv(r'./interfaces/data/manufacturer_wind.csv', index_col=0, sep=';')
        #self.wind_types = pd.read_excel(r'./y.xlsx')
        #self.manufacturer = self.wind_types['manu'].unique()

    def get_power_plant_in_area(self, area=520, fuel_type='lignite'):

        mastr_codes_fossil = pd.read_csv(r'./interfaces/data/mastr_codes_fossil.csv', index_col=0)

        if area in self.centers.keys():
            longitude, latitude = self.centers[area]

            query = f'SELECT ev."EinheitMastrNummer" as "unitID", ' \
                    f'ev."Energietraeger" as "fuel", ' \
                    f'COALESCE(ev."Laengengrad", {longitude}) as "lon", ' \
                    f'COALESCE(ev."Breitengrad", {latitude}) as "lat", ' \
                    f'COALESCE(ev."Inbetriebnahmedatum", \'2010-01-01\') as "startDate", ' \
                    f'ev."Nettonennleistung" as "maxPower", ' \
                    f'ev."Technologie" as "turbineTyp", ' \
                    f'ev."GenMastrNummer" as "generatorID"'
            if fuel_type != 'nuclear':
                query += f', ' + \
                         f'kwk."ThermischeNutzleistung" as "kwkPowerTherm", ' + \
                         f'kwk."ElektrischeKwkLeistung" as "kwkPowerElec", ' + \
                         f'ev."AnlageIstImKombibetrieb" as "combination" ' + \
                         f'FROM "EinheitenVerbrennung" ev ' + \
                         f'LEFT JOIN "AnlagenKwk" kwk ON kwk."KwkMastrNummer" = ev."KwkMastrNummer" ' + \
                         f'WHERE ev."Postleitzahl" >= {area * 100} AND ev."Postleitzahl" < {area * 100 + 100} ' + \
                         f'AND ev."Energietraeger" = {mastr_codes_fossil.loc[fuel_type, "value"]} ' + \
                         f'AND ev."Nettonennleistung" > 5000 AND ev."EinheitBetriebsstatus" = 35 ' + \
                         f'AND ev."ArtDerStilllegung" isnull;'
            else:
                query += f' ' + \
                         f'FROM "EinheitenKernkraft" ev ' + \
                         f'WHERE ev."Postleitzahl" >= {area * 100} AND ev."Postleitzahl" < {area * 100 + 100} ' \

            # Get Data from Postgres
            df = pd.read_sql(query, self.database_mastr)

            if not df.empty:
                type_years = np.asarray([0, 2000, 2018])  # technical setting typ
                # set default values for technical parameters
                type_years = np.asarray([0, 2000, 2018])                    # technical setting typ
                df['fuel'] = fuel_type                                      # current fuel typ
                df['maxPower'] = df['maxPower'] / 10**3                     # Rated Power [kW] --> [MW]
                df['minPower'] = df['maxPower'] * 0.5                       # MinPower = 1/2 MaxPower
                df['P0'] = df['minPower'] + 0.1
                df['gradP'] = 0.1 * df['maxPower']                          # 10% Change per hour
                df['gradM'] = 0.1 * df['maxPower']                          # 10% Change per hour
                df['stopTime'] = 5                                          # default stop time 5h
                df['runTime'] = 5                                           # default run time 5h
                df['on'] = 1                                                # on counter --> Plant is on till 1 hour
                df['off'] = 0                                               # off counter --> Plant is on NOT off
                df['eta'] = 30                                              # efficiency
                df['chi'] = 1.                                              # emission factor [t/MWh therm]
                df['startCost'] = 100 * df['maxPower']                      # starting cost [€/MW Rated]
                df['turbineTyp'] = [mastr_codes_fossil.loc[str(x), 'value'] # convert int to string
                                    if x is not None else None for x in df['turbineTyp'].to_numpy(int)]
                df['startDate'] = [pd.to_datetime(x) if x is not None       # convert to timestamp and set default to 2005
                                   else pd.to_datetime('2005-05-05') for x in df['startDate']]
                if 'combination' in df.columns:                             # if no combination flag is set, set it to 0
                    df['combination'] = [0 if x == 0 or x is None else 1 for x in df['combination']]
                else:                                                       # Add column for nuclear power plants
                    df['combination'] = 0
                df['type'] = [type_years[type_years < x.year][-1] for x in df['startDate']]
                df['generatorID'] = [id_ if id_ is not None else 0          # set default generatorID to 0
                                     for id_ in df['generatorID']]
                if 'kwkPowerTherm' in df.columns:
                    df['kwkPowerTherm'] = df['kwkPowerTherm'].fillna(0)
                    df['kwkPowerTherm'] = df['kwkPowerTherm'] / 10**3
                else:
                    df['kwkPowerTherm'] = 0
                if 'kwkPowerElec' in df.columns:
                    df['kwkPowerElec'] = df['kwkPowerElec'].fillna(0)
                    df['kwkPowerElec'] = df['kwkPowerElec'] / 10**3
                else:
                    df['kwkPowerElec'] = 0

                # for all gas turbines check if they are used in an combination of gas and steam turbine
                if fuel_type == 'gas':
                    # CCHP Power Plant with Combination
                    cchps = df[df['combination'] == 1]
                    new_cchps = []
                    # aggregate with generatorID
                    for genID in cchps['generatorID'].unique():
                        if genID != 0:
                            cchp = cchps[cchps['generatorID'] == genID]
                            cchp.index = range(len(cchp))
                            cchp.at[0, 'maxPower'] = sum(cchp['maxPower'])
                            cchp.at[0, 'kwkPowerTherm'] = sum(cchp['kwkPowerTherm'])
                            cchp.at[0, 'kwkPowerElec'] = sum(cchp['kwkPowerElec'])
                            cchp.at[0, 'turbineTyp'] = 'Closed Cycle Heat Power'
                            cchp.at[0, 'fuel'] = 'gas_combined'
                            new_cchps.append(cchp.loc[0, cchp.columns])             # only append the aggregated row!
                        else:
                            cchp = cchps[cchps['generatorID'] == 0]
                            cchp['turbineTyp'] = 'Closed Cycle Heat Power'
                            cchp['fuel'] = 'gas_combined'
                            for line in range(len(cchp)):
                                new_cchps.append(cchp.iloc[line])                   # append all rows

                    # combine the gas turbines without combination flag with the new created
                    df = pd.concat([df[df['combination'] == 0], pd.DataFrame(new_cchps)])
                    df.index = range(len(df))

                    # check the gas turbines with non set combination flag but turbine = typ Closed Cycle Heat Power
                    for line, row in df.iterrows():
                        if all([row['combination'] == 0, row['turbineTyp'] == 'Closed Cycle Heat Power',
                                row['fuel'] == 'gas']):
                            df.at[line, 'fuel'] = 'gas_combined'

                # Set technical parameter corresponding to the type (0, 2000, 2018)
                technical_parameter = pd.read_csv(fr'./interfaces/data/technical_parameter_{fuel_type}.csv',
                                                  sep=';', decimal=',', index_col=0)

                for line, row in df.iterrows():
                    df.at[line, 'minPower'] = df.at[line, 'maxPower'] * technical_parameter.at[row['type'], 'minPower'] / 100
                    df.at[line, 'P0'] = df.at[line, 'minPower']
                    df.at[line, 'gradP'] = np.round(df.at[line, 'maxPower'] * technical_parameter.at[row['type'], 'gradP'] * 60 / 100,2)
                    df.at[line, 'gradM'] = np.round(df.at[line, 'maxPower'] * technical_parameter.at[row['type'], 'gradM'] * 60 / 100,2)
                    df.at[line, 'eta'] = technical_parameter.at[row['type'], 'eta']
                    df.at[line, 'chi'] = technical_parameter.at[row['type'], 'chi']
                    df.at[line, 'stopTime'] = technical_parameter.at[row['type'], 'stopTime']
                    df.at[line, 'runTime'] = technical_parameter.at[row['type'], 'runTime']
                    df.at[line, 'startCost'] = df.at[line, 'maxPower'] * technical_parameter.at[row['type'], 'startCost']

                return df

        return pd.DataFrame()

    def get_solar_systems_in_area(self, area=520, solar_type='roof_top'):

        mastr_codes_solar = pd.read_csv(r'./interfaces/data/mastr_codes_solar.csv', index_col=0)

        if area in self.centers.keys():
            longitude, latitude = self.centers[area]

            query = f'SELECT "EinheitMastrNummer" as "unitID", ' \
                    f'"Nettonennleistung" as "maxPower", ' \
                    f'COALESCE("Laengengrad", {longitude}) as "lon", ' \
                    f'COALESCE("Breitengrad", {latitude}) as "lat", ' \
                    f'COALESCE("Hauptausrichtung", 699) as "azimuthCode", ' \
                    f'"Leistungsbegrenzung" as "limited", ' \
                    f'"Einspeisungsart" as "ownConsumption", ' \
                    f'COALESCE("HauptausrichtungNeigungswinkel", 809) as "tiltCode", ' \
                    f'COALESCE("Inbetriebnahmedatum", \'2018-01-01\') as "startDate",' \
                    f'"InanspruchnahmeZahlungNachEeg" as "eeg" ' \
                    f'FROM "EinheitenSolar" ' \
                    f'INNER JOIN "AnlagenEegSolar" ON "EinheitMastrNummer" = "VerknuepfteEinheitenMastrNummern" ' \
                    f'WHERE "Postleitzahl" >= {area * 100} AND "Postleitzahl" < {area * 100 + 100} ' \
                    f'AND "Lage" = {mastr_codes_solar.loc[solar_type, "value"]} ' \
                    f'AND "EinheitBetriebsstatus" = 35;'

            # Get Data from Postgres
            df = pd.read_sql(query, self.database_mastr)
            # If the response Dataframe is not empty set technical parameter
            if not df.empty:
                # all PVs with are implemented in 2018
                df['startDate'] = pd.to_datetime(df['startDate'], infer_datetime_format=True)
                # all PVs with nan are south oriented assets
                df['azimuth'] = [mastr_codes_solar.loc[str(code), 'value'] for code in df['azimuthCode'].to_numpy(int)]
                del df['azimuthCode']
                # all PVs with nan have a tilt angle of 30°
                df['tilt'] = [mastr_codes_solar.loc[str(code), 'value'] for code in df['tiltCode'].to_numpy(int)]
                del df['tiltCode']
                if solar_type == 'roof_top':
                    # all PVs with nan and startDate > 2013 have ownConsumption
                    missing_values = df['ownConsumption'].isna()
                    deadline = [date.year > 2013 for date in df['startDate']]
                    own_consumption = [all([missing_values[i], deadline[i]]) for i in range(len(missing_values))]
                    df.loc[own_consumption, 'ownConsumption'] = 1
                    grid_use = [all([missing_values[i], not deadline[i]]) for i in range(len(missing_values))]
                    df.loc[grid_use, 'ownConsumption'] = 0
                    df['ownConsumption'] = df['ownConsumption'].replace(689, 1)
                    df['ownConsumption'] = df['ownConsumption'].replace(688, 0)
                    # assumption "regenerative Energiesysteme"
                    df['demandP'] = df['maxPower'] * 10 ** 3
                elif solar_type == 'free_area' or solar_type == 'other':
                    # set own consumption for free area mounted PVs to 0, because the demand is unknown
                    df['ownConsumption'] = 0
                if solar_type == 'roof_top':
                    # all PVs with nan and startDate > 2012 and maxPower > 30 are limited to 70%
                    missing_values = df['limited'].isna()
                    power_cap = df['maxPower'] > 30
                    deadline = [date.year > 2012 for date in df['startDate']]
                    limited = [all([missing_values[i], deadline[i], power_cap[i]]) for i in range(len(missing_values))]
                    df.loc[limited, 'limited'] = 803
                    # rest nans have no limitation
                    df['limited'] = df['limited'].fillna(802)
                    df['limited'] = [mastr_codes_solar.loc[str(code), 'value'] for code in df['limited'].to_numpy(int)]
                if solar_type == 'free_area' or solar_type == 'other':
                    # TODO: Check restrictions for free area pv
                    # nans have no limitation
                    df['limited'] = df['limited'].fillna(802)
                    df['limited'] = [mastr_codes_solar.loc[str(code), 'value'] for code in df['limited'].to_numpy(int)]
                # all PVs with nan and startDate > 2016 and maxPower > 100 have direct marketing
                missing_values = df['eeg'].isna()
                power_cap = df['maxPower'] > 100
                deadline = [date.year > 2016 for date in df['startDate']]
                eeg = [all([missing_values[i], deadline[i], power_cap[i]]) for i in range(len(missing_values))]
                df.loc[eeg, 'eeg'] = 0
                # rest nans are eeg assets and are managed by the tso
                df['eeg'] = df['eeg'].replace(np.nan, 0)
                # set max Power to [MW]
                df['maxPower'] = df['maxPower'] / 10**3
                return df

            return pd.DataFrame()

    def get_wind_turbines_in_area(self, area=520, wind_type='on_shore'):

        mastr_codes_wind = pd.read_csv(r'./interfaces/data/mastr_codes_wind.csv', index_col=0)

        if area in self.centers.keys():
            longitude, latitude = self.centers[area]

            query = f'SELECT "EinheitMastrNummer" as "unitID", ' \
                    f'"Nettonennleistung" as "maxPower", ' \
                    f'COALESCE("Laengengrad", {longitude}) as "lon", ' \
                    f'COALESCE("Breitengrad", {latitude}) as "lat", ' \
                    f'"Typenbezeichnung" as "typ", ' \
                    f'COALESCE("Hersteller", -1) as "manufacturer", ' \
                    f'"Nabenhoehe" as "height", ' \
                    f'"Rotordurchmesser" as "diameter", ' \
                    f'"ClusterNordsee" as "nordicSea", ' \
                    f'"ClusterOstsee" as "balticSea", ' \
                    f'"GenMastrNummer" as "generatorID", ' \
                    f'COALESCE("Inbetriebnahmedatum", \'2018-01-01\') as "startDate" ' \
                    f'FROM "EinheitenWind" ' \
                    f'WHERE "EinheitBetriebsstatus" = 35 ' \
                    f'AND "Lage" = {mastr_codes_wind.loc[wind_type, "value"]}'
            if wind_type == 'on_shore':
                query += f' AND "Postleitzahl" >= {area * 100} AND "Postleitzahl" < {area * 100 + 100};'

            # Get Data from Postgres
            df = pd.read_sql(query, self.database_mastr)
            # If the response Dataframe is not empty set technical parameter
            if not df.empty:
                # set max Power to [MW]
                df['maxPower'] = df['maxPower'] / 10**3
                # all WEA with nan set hight to mean value
                df['height'] = df['height'].fillna(df['height'].mean())
                # all WEA with nan set hight to mean diameter
                df['diameter'] = df['diameter'].fillna(df['diameter'].mean())
                # all WEA with na are on shore and not allocated to a sea cluster
                df['nordicSea'] = df['nordicSea'].fillna(0)
                df['balticSea'] = df['balticSea'].fillna(0)
                # get name of manufacturer
                df['manufacturer'] = [mastr_codes_wind.loc[str(x), 'value'] for x in df['manufacturer'].to_numpy(int)]
                # try to find the correct type TODO: Check Pattern of new turbines
                #df['typ'] = [str(typ).replace(' ', '').replace('-', '').upper() for typ in df['typ']]
                #df['typ'] = [None if re.search(self.pattern_wind, typ) is None else re.search(self.pattern_wind, typ).group()
                #             for typ in df['typ']]
                # df['typ'] = df['typ'].replace('', 'default')
                # set tag for wind farms
                wind_farm_prefix = f'{area * 1000}F'
                df['windFarm'] = 'x'
                counter = 0
                for genId in df['generatorID'].unique():
                    if genId is not None and len(df[df['generatorID'] == genId]) > 1:
                        windFarm = df[df['generatorID'] == genId]
                        for line, row in windFarm.iterrows():
                            df.at[line, 'windFarm'] = f'{wind_farm_prefix}{counter}'
                        counter += 1
                return df

        return pd.DataFrame()

    def get_biomass_systems_in_area(self, area=520):

        if area in self.centers.keys():
            longitude, latitude = self.centers[area]

            # TODO: Add more Parameters, if the model get more complex
            query = f'SELECT "EinheitMastrNummer" as "unitID", ' \
                    f'COALESCE("Inbetriebnahmedatum", \'2018-01-01\') as "startDate", ' \
                    f'"Nettonennleistung" as "maxPower", ' \
                    f'COALESCE("Laengengrad", {longitude}) as "lon", ' \
                    f'COALESCE("Breitengrad", {latitude}) as "lat" ' \
                    f'FROM "EinheitenBiomasse"' \
                    f'WHERE "Postleitzahl" >= {area * 100} AND "Postleitzahl" < {area * 100 + 100} AND ' \
                    f'"EinheitBetriebsstatus" = 35 ;'

            # Get Data from Postgres
            df = pd.read_sql(query, self.database_mastr)
            # If the response Dataframe is not empty set technical parameter
            if not df.empty:
                df['maxPower'] = df['maxPower'] / 10**3

                return df

        return pd.DataFrame()

    def get_run_river_systems_in_area(self, area=520):

        if area in self.centers.keys():
            longitude, latitude = self.centers[area]

            # TODO: Add more Parameters, if the model get more complex
            query = f'SELECT "EinheitMastrNummer" as "unitID", ' \
                    f'COALESCE("Inbetriebnahmedatum", \'2018-01-01\') as "startDate", ' \
                    f'"Nettonennleistung" as "maxPower", ' \
                    f'COALESCE("Laengengrad", {longitude}) as "lon", ' \
                    f'COALESCE("Breitengrad", {latitude}) as "lat" ' \
                    f'FROM "EinheitenWasser"' \
                    f'WHERE "Postleitzahl" >= {area * 100} AND "Postleitzahl" < {area * 100 + 100} AND ' \
                    f'"EinheitBetriebsstatus" = 35 AND "ArtDerWasserkraftanlage" = 890'

            # Get Data from Postgres
            df = pd.read_sql(query, self.database_mastr)
            # If the response Dataframe is not empty set technical parameter
            if not df.empty:
                df['maxPower'] = df['maxPower'] / 10**3

                return df

        return pd.DataFrame()

    def get_water_storage_systems(self, area=800):

        storage_volumes = pd.read_csv(r'./interfaces/data/technical_parameter_storage.csv', index_col=0)

        if area in self.centers.keys():
            longitude, latitude = self.centers[area]

            query = f'SELECT "EinheitMastrNummer" as "unitID", ' \
                    f'"LokationMastrNummer" as "locationID", ' \
                    f'"SpeMastrNummer" as "storageID", ' \
                    f'"NameStromerzeugungseinheit" as "name", ' \
                    f'COALESCE("Inbetriebnahmedatum", \'2018-01-01\') as "startDate", ' \
                    f'"Nettonennleistung" as "P-_max", ' \
                    f'"NutzbareSpeicherkapazitaet" as "VMax", ' \
                    f'"PumpbetriebLeistungsaufnahme" as "P+_max", ' \
                    f'COALESCE("Laengengrad", {longitude}) as "lon", ' \
                    f'COALESCE("Breitengrad", {latitude}) as "lat" ' \
                    f'FROM "EinheitenStromSpeicher"' \
                    f'LEFT JOIN "AnlagenStromSpeicher" ON "EinheitMastrNummer" = "VerknuepfteEinheitenMastrNummern" ' \
                    f'WHERE "Postleitzahl" >= {area * 100} AND "Postleitzahl" < {area * 100 + 100} AND ' \
                    f'"EinheitBetriebsstatus" = 35 AND "Technologie" = 1537 AND "EinheitSystemstatus"=472 AND "Land"=84 ' \
                    f'AND "Nettonennleistung" > 500' \

            # Get Data from Postgres
            df = pd.read_sql(query, self.database_mastr)
            # If the response Dataframe is not empty set technical parameter
            if not df.empty:
                # set charge and discharge power
                df['P+_max'] = df['P+_max'].fillna(df['P-_max'])            # fill na with Rated Power
                df['P-_max'] = df['P-_max'] / 10**3                         # [kW] --> [MW]
                df['P-_min'] = 0                                            # set min to zero
                df['P+_max'] = df['P+_max'] / 10**3                         # [kW] --> [MW]
                df['P+_min'] = 0                                            # set min to zero

                # fill nan values with default from wiki
                df['VMax'] = df['VMax'].fillna(0)
                df['VMax'] = df['VMax'] / 10**3
                for index, row in df[df['VMax'] == 0].iterrows():
                    for key, value in storage_volumes.iterrows():
                        if key in row['name']:
                            df.at[index, 'VMax'] = value['volume']

                storages = []
                for id_ in df['storageID'].unique():
                    data = df[df['storageID'] == id_]
                    storage = {'unitID': id_,
                               'startDate': pd.to_datetime(data['startDate'].to_numpy()[0]),
                               'P-_max': data['P-_max'].sum(),
                               'P+_max': data['P+_max'].sum(),
                               'VMax': data['VMax'].to_numpy()[0],
                               'VMin': 0,
                               'V0': data['VMax'].to_numpy()[0]/2,
                               'lat': data['lat'].to_numpy()[0],
                               'lon': data['lon'].to_numpy()[0],
                               'eta+': 0.85,
                               'eta-': 0.80}
                    storages.append(storage)

                return pd.DataFrame(storages)

        return pd.DataFrame()

    def get_demand_in_area(self, area=520):
        # TODO: Check Enet Data for better Estimation
        grids = {'nsp': [], 'msp': []}
        val = {'nsp': 'netz_nsp', 'msp': 'netz_nr_msp'}

        for voltage_level in ['nsp', 'msp']:
            query = f'SELECT distinct (no.ortsteil, no.ort, {val[voltage_level]}) ' \
                    f'FROM netze_ortsteile no where plz >= {area*100} and plz < {area*100+100} ' \
                    f'and no.gueltig_bis = \'2100-01-01 00:00:00\''

            df = pd.read_sql(query, self.database_enet)
            if not df.empty:
                for _, series in df.iterrows():
                    x = tuple(map(str, series.values[0][1:-1].split(',')))
                    grid_id = int(x[-1])
                    if grid_id not in grids[voltage_level]:
                        grids[voltage_level].append(grid_id)

        val = {'nsp': 'arbeit_ns', 'msp': 'arbeit_ms'}
        demand_factor = 0
        nsp = 0
        msp = 0
        for key, value in grids.items():
            for grid in value:
                query = f'SELECT {val[key]} FROM netzdaten where netz_nr = {grid} order by stand desc limit 1 '
                df = pd.read_sql(query, self.database_enet)
                if not df.empty:
                    df = df.fillna(0)
                    demand_factor += df[val[key]].to_numpy()[0]
                    if key == 'nsp':
                        nsp += df[val[key]].to_numpy()[0]
                    else:
                        msp += df[val[key]].to_numpy()[0]

        return 540*(demand_factor/10**6)/2668.37, nsp/(nsp+msp), msp/(nsp+msp)        # --> 2668.37 = Sum over all PLZ Areas

    def get_solar_storage_systems_in_area(self, area=521):

        mastr_codes_solar = pd.read_csv(r'./interfaces/data/mastr_codes_solar.csv', index_col=0)

        if area in self.centers.keys():
            longitude, latitude = self.centers[area]

            query = f'SELECT spe."LokationMastrNummer" as "unitID", ' \
                    f'so."Nettonennleistung" as "maxPower", ' \
                    f'spe."Nettonennleistung" as "batPower", ' \
                    f'COALESCE(so."Laengengrad", {longitude}) as "lon", ' \
                    f'COALESCE(so."Breitengrad", {latitude}) as "lat", ' \
                    f'COALESCE(so."Hauptausrichtung", 699) as "azimuthCode", ' \
                    f'COALESCE(so."Leistungsbegrenzung", 802) as "limited", ' \
                    f'COALESCE(so."Einspeisungsart", 689) as "ownConsumption", ' \
                    f'COALESCE(so."HauptausrichtungNeigungswinkel", 809) as "tiltCode", ' \
                    f'COALESCE(so."Inbetriebnahmedatum", \'2018-01-01\') as "startDate", ' \
                    f'an."NutzbareSpeicherkapazitaet" as "VMax" ' \
                    f'FROM "EinheitenStromSpeicher" spe ' \
                    f'INNER JOIN "EinheitenSolar" so ON spe."LokationMastrNummer" = so."LokationMastrNummer" ' \
                    f'INNER JOIN "AnlagenStromSpeicher" an ON spe."SpeMastrNummer" = an."MastrNummer"' \
                    f'WHERE so."Postleitzahl" >= {area * 100} AND so."Postleitzahl" < {area * 100 + 100} ' \
                    f'AND so."EinheitBetriebsstatus" = 35;'

            # Get Data from Postgres
            df = pd.read_sql(query, self.database_mastr)
            # If the response Dataframe is not empty set technical parameter
            if not df.empty:
                df['VMax'] = df['VMax'].fillna(10)
                df['ownConsumption'] = df['ownConsumption'].replace(689, 1)
                df['ownConsumption'] = df['ownConsumption'].replace(688, 0)
                df['limited'] = [mastr_codes_solar.loc[str(code), 'value'] for code in df['limited'].to_numpy(int)]

                # all PVs with nan are south oriented assets
                df['azimuth'] = [mastr_codes_solar.loc[str(code), 'value'] for code in df['azimuthCode'].to_numpy(int)]
                del df['azimuthCode']
                # all PVs with nan have a tilt angle of 30°
                df['tilt'] = [mastr_codes_solar.loc[str(code), 'value'] for code in df['tiltCode'].to_numpy(int)]
                del df['tiltCode']
                # assumption "regenerative Energiesysteme"
                df['demandP'] = df['maxPower'] * 10**3

                df['eta'] = 0.96
                df['V0'] = 0

                return df

        return pd.DataFrame()

    def get_position(self, area):
        if area in self.centers.keys():
            return self.centers[area]
        return None, None

if __name__ == "__main__":
    import os
    import numpy as np
    from tqdm import tqdm
    x = os.getenv('INFRASTRUCTURE_SOURCE', '10.13.10.41:5432')
    y = os.getenv('INFRASTRUCTURE_LOGIN', 'opendata:opendata')

    interface = InfrastructureInterface(infrastructure_source=x, infrastructure_login=y)
    x = interface.get_power_plant_in_area(area=415, fuel_type='gas')
    y = interface.get_water_storage_systems(area=415)
    z = interface.get_solar_storage_systems_in_area(area=415)
    a = interface.get_solar_systems_in_area(area=415)
    # pwp_agents = []
    # for plz in interface.centers.keys():
    #     print(plz)
    #     plants = False
    #     for fuel in ['lignite', 'gas', 'coal', 'nuclear']:
    #         df = interface.get_power_plant_in_area(area=plz, fuel_typ=fuel)
    #         if not df.empty:
    #             plants = True
    #             break
    #     if plants:
    #         pwp_agents.append(plz)
    # pwp_agents = np.asarray(pwp_agents)
    # np.save('pwp_agents', pwp_agents)
    #
    # res_agents = []
    # for plz in interface.centers.keys():
    #     print(plz)
    #     plants = False
    #     wind = interface.get_wind_turbines_in_area(area=plz)
    #     solar = interface.get_solar_storage_systems_in_area(area=plz)
    #     bio = interface.get_solar_storage_systems_in_area(area=plz)
    #     water = interface.get_run_river_systems_in_area(area=plz)
    #     if any([not wind.empty,not solar.empty,not bio.empty, not water.empty]):
    #         res_agents.append(plz)
    #
    # res_agents = np.asarray(res_agents)
    # np.save('res_agents', res_agents)
    #
    # dem_agents = []
    # for plz in interface.centers.keys():
    #     dem_agents.append(plz)
    # dem_agents = np.asarray(dem_agents)
    # np.save('dem_agents', dem_agents)
    #
    # str_agents = []
    # for plz in interface.centers.keys():
    #     print(plz)
    #     str = interface.get_water_storage_systems(plz)
    #     if not str.empty:
    #         str_agents.append(plz)
    #
    # str_agents = np.asarray(str_agents)
    # np.save('str_agents', str_agents)