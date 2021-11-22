from sqlalchemy import create_engine
import pandas as pd


class Infrastructure:

    def __init__(self, user='opendata', password='opendata', database='mastr', host='10.13.10.41', port=5432):
        self.engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
        # MaStR Codes for different fuel types used in Power Plant Table
        # TODO: Check if 2413 is Combined Gas and Steam Systems
        self.fuel_codes = {
            'coal':         2407,
            'lignite':      2408,
            'oil':          2409,
            'gas':          2410,
            'landfill_gas': 2411,
            'waste':        2412
        }

        self.technical_type = {
            1990: 'typ1',
            2000: 'typ2',
            2018: 'typ3'
        }

        self.technical_parameter = {
            'coal': {
                'typ1': dict(up_time=8, down_time=9, gradient=1.5/100, MinPower=0.40, eta=0.36),
                'typ2': dict(up_time=6, down_time=7, gradient=4/100, MinPower=0.33, eta=0.40),
                'typ3': dict(up_time=4, down_time=5, gradient=6/100, MinPower=0.25, eta=0.45),
            },
            'lignite': {
                'typ1': dict(up_time=8, down_time=9, gradient=1/100, MinPower=0.60, eta=0.34),
                'typ2': dict(up_time=6, down_time=7, gradient=2.5/100, MinPower=0.50, eta=0.40),
                'typ3': dict(up_time=4, down_time=5, gradient=4/100, MinPower=0.40, eta=0.45),
            },
            'nuclear': {
                'typ1': dict(up_time=20, down_time=24, gradient=3/100, MinPower=0.50, eta=0.33),
                'typ2': dict(up_time=15, down_time=24, gradient=3/100, MinPower=0.45, eta=0.35),
                'typ3': dict(up_time=10, down_time=24, gradient=3/100, MinPower=0.40, eta=0.38),
            },
            'gas': {
                'typ1': dict(up_time=4, down_time=4, gradient=8/100, MinPower=0.40, eta=0.40),
                'typ2': dict(up_time=3, down_time=3, gradient=12/100, MinPower=0.40, eta=0.45),
                'typ3': dict(up_time=2, down_time=2, gradient=15/100, MinPower=0.40, eta=0.50),
            },
            # 'gas_combined': {
            #     'typ1': dict(up_time=4, down_time=4, gradient=2, power_min=0.40, eta=0.45),
            #     'typ2': dict(up_time=15, down_time=3, gradient=4, power_min=0.40, eta=0.55),
            #     'typ3': dict(up_time=10, down_time=2, gradient=8, power_min=0.40, eta=0.61)
            # }
        }

    def get_power_plant_in_area(self, area=52, fuel_typ='lignite'):
        start = area * 1000
        end = start + 1000
        if fuel_typ != 'nuclear':
            typ = self.fuel_codes[fuel_typ]
            query = f'SELECT "EinheitMastrNummer" as "UnitID", ' \
                    f'"LokationMastrNummer" as "LocationID", ' \
                    f'"NameKraftwerk" as "Name", ' \
                    f'"NameKraftwerksblock" as "BlockName", ' \
                    f'"Kraftwerksnummer" as "PowerPlantNumber", ' \
                    f'"Energietraeger" as "fuel", ' \
                    f'"Laengengrad" as "lat", ' \
                    f'"Breitengrad" as "lon", ' \
                    f'"AnlageIstImKombibetrieb" as "CHP", ' \
                    f'"KwkMastrNummer" as "KWKNumber", ' \
                    f'"Inbetriebnahmedatum" as "StartDate", ' \
                    f'"Nettonennleistung" as "RatedPower" ' \
                    f'FROM "EinheitenVerbrennung" ' \
                    f'WHERE "Postleitzahl" >= {start} AND "Postleitzahl" < {end} ' \
                    f'AND "Energietraeger" = {typ} ' \
                    f'AND "Nettonennleistung" > 100 AND "EinheitBetriebsstatus" = 35;'
        else:
            query = ''

        df = pd.read_sql(query, self.engine)
        df['fuel'] = fuel_typ
        df['RatedPower'] = df['RatedPower']/10**3

        parameters = []
        try:
            for date in df['StartDate'].to_numpy():
                year = pd.to_datetime(date).year
                type = 'typ1'
                for key in self.technical_type.keys():
                    if year > key:
                        type = self.technical_type[key]
                parameters.append(self.technical_parameter[fuel_typ][type])

            tech_df = pd.DataFrame(parameters)

            for column in tech_df.columns:
                if column == 'gradient':
                    df[column] = tech_df[column] * 60 * df['RatedPower']
                if column == 'MinPower':
                    df[column] = tech_df[column] * df['RatedPower']

        except Exception as e:
            print(e)

        return df

    def get_wind_turbines_in_area(self, area=50):
        pass

    def get_free_solar_systems_in_area(self, area=50):
        start = area * 1000
        end = start + 1000
        # MaStR Code 852 --> free area
        # TODO: Check https://mein-pv-anwalt.de/bauliche-anlage/ Status Code 2484 Sonstige Bauliche Anlagen
        query = f'SELECT "LokationMastrNummer" as ID, sum("Nettonennleistung") as Power, sum("AnzahlModule") as Module,' \
                f'avg("Laengengrad") as lat, avg("Breitengrad") as lon FROM "EinheitenSolar" WHERE ' \
                f'"Postleitzahl" >= {start} AND "Postleitzahl" < {end} AND "Lage" = 852 ' \
                f'AND "EinheitBetriebsstatus" = 35 GROUP BY "LokationMastrNummer";'
        df = pd.read_sql(query, self.engine).iloc[:-2]
        df['azimuth'] = 180
        df['tilt'] = 30

        return df

    def get_roof_top_solar_systems_in_area(self, area=50):
        start = area * 1000
        end = start + 1000
        # MaStR Code 853 --> Roof Top
        query = f'SELECT "EinheitMastrNummer" as ID, ' \
                f'"Nettonennleistung" as Power, ' \
                f'"AnzahlModule" as Module,' \
                f'"Laengengrad" as lat, ' \
                f'"Breitengrad" as lon, ' \
                f'"Hauptausrichtung" as azimuth, ' \
                f'"Leistungsbegrenzung", ' \
                f'"Einspeisungsart", ' \
                f'"FernsteuerbarkeitNb", ' \
                f'"HauptausrichtungNeigungswinkel", ' \
                f'"Inbetriebnahmedatum", ' \
                f'"InanspruchnahmeZahlungNachEeg" as "EEG" ' \
                f'FROM "EinheitenSolar" ' \
                f'INNER JOIN "AnlagenEegSolar" ON "EinheitMastrNummer" = "VerknuepfteEinheitenMastrNummern" ' \
                f'WHERE "Postleitzahl" >= {start} AND "Postleitzahl" < {end} AND "Lage" = 853 ' \
                f'AND "EinheitBetriebsstatus" = 35;'

        return pd.read_sql(query, self.engine)

    def get_biomass_systems_in_area(self, area=50):
        start = area * 1000
        end = start + 1000
        '''
        EinheitenMastrNummer --> Unique for each Gen.-Unit
        LokationMastrNummer --> ID for more Gen.-Unit aggregated
        Inbetriebnahmedatum --> Date of commissioning
        FernsteuerbarkeitNb --> TSO Control
        Einspeisungsart --> owen consumption (true/false)
        Biomasseart --> Type of Biomass
        EegMastrNummer --> Unique Number in EEGAnlagen Table
        KwkMastrNummer --> Unique Number in KWKAnlagen Table
        Laengengrad --> latitude
        Breitengrad --> longitude
        FernsteuerbarkeitDv --> Control via direct marketing
        FernsteuerbarkeitDr --> Control via third
        "Nettonennleistung" --> elect. Power
        '''

        query = f'SELECT "EinheitMastrNummer" as "ID", ' \
                f'"LokationMastrNummer" as "PowerUnit", ' \
                f'"Inbetriebnahmedatum" as "Date", ' \
                f'"FernsteuerbarkeitNb" as "GridControl", ' \
                f'"Einspeisungsart" as "Consumption", ' \
                f'"Biomasseart" as "BioType", ' \
                f'"EegMastrNummer" "EEGNumber", ' \
                f'"KwkMastrNummer" "KWKNumber", ' \
                f'"Nettonennleistung" as "Power", ' \
                f'"Laengengrad" as "lat", ' \
                f'"Breitengrad" as "lot", ' \
                f'"FernsteuerbarkeitDv" as "MarketControl", ' \
                f'"FernsteuerbarkeitDr" as "ThirdControl", ' \
                f'"InanspruchnahmeZahlungNachEeg" as "EEG" ' \
                f'FROM "EinheitenBiomasse"' \
                f'INNER JOIN "AnlagenEegBiomasse" ON "EinheitMastrNummer" = "VerknuepfteEinheitenMastrNummern" ' \
                f'WHERE "Postleitzahl" >= {start} AND "Postleitzahl" < {end} AND ' \
                f'"EinheitBetriebsstatus" = 35 '

        df = pd.read_sql(query, self.engine)
        return df

    def get_run_river_systems_in_area(self, area=50):
        start = area * 1000
        end = start + 1000
        '''
        EinheitenMastrNummer --> Unique for each Gen.-Unit
        LokationMastrNummer --> ID for more Gen.-Unit aggregated
        Inbetriebnahmedatum --> Date of commissioning
        FernsteuerbarkeitNb --> TSO Control
        Einspeisungsart --> owen consumption (true/false)
        EegMastrNummer --> Unique Number in EEGAnlagen Table
        Laengengrad --> latitude
        Breitengrad --> longitude
        FernsteuerbarkeitDv --> Control via direct marketing
        FernsteuerbarkeitDr --> Control via third
        "Nettonennleistung" --> elect. Power
        '''

        query = f'SELECT "EinheitMastrNummer" as "ID", ' \
                f'"LokationMastrNummer" as "PowerUnit", ' \
                f'"Inbetriebnahmedatum" as "Date", ' \
                f'"FernsteuerbarkeitNb" as "GridControl", ' \
                f'"Einspeisungsart" as "Consumption", ' \
                f'"EegMastrNummer" "EEGNumber", ' \
                f'"Nettonennleistung" as "Power", ' \
                f'"Laengengrad" as "lat", ' \
                f'"Breitengrad" as "lot", ' \
                f'"FernsteuerbarkeitDv" as "MarketControl", ' \
                f'"FernsteuerbarkeitDr" as "ThirdControl", ' \
                f'"InanspruchnahmeZahlungNachEeg" as "EEG" ' \
                f'FROM "EinheitenWasser"' \
                f'INNER JOIN "AnlagenEegWasser" ON "EinheitMastrNummer" = "VerknuepfteEinheitenMastrNummern" ' \
                f'WHERE "Postleitzahl" >= {start} AND "Postleitzahl" < {end} AND ' \
                f'"EinheitBetriebsstatus" = 35 '

        df = pd.read_sql(query, self.engine)
        return df


if __name__ == "__main__":
    interface = Infrastructure()

    codes = interface.fuel_codes

    power_plants = []
    for key, _ in codes.items():
        p_plants = interface.get_power_plant_in_area(area=52, fuel_typ=key)
        power_plants.append({key: p_plants})

    # biomass = interface.get_biomass_systems_in_area(area=20)
    #runriver = interface.get_run_river_systems_in_area(area=52)

    #pvR = interface.get_roof_top_solar_systems_in_area(area=52)
    #pcF = interface.get_free_solar_systems_in_area(area=52)
