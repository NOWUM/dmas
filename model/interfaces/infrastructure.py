from sqlalchemy import create_engine
import pandas as pd


class Infrastructure:

    def __init__(self, user='opendata', password='opendata', database='mastr', host='10.13.10.41', port=5432):
        self.engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

    def get_lignite_power_plants_in_area(self, area=50):
        start = area * 1000
        end = start + 1000
        # MaStR Code 2408 --> Lignite
        query = f'SELECT "EinheitMastrNummer" as "ID", ' \
                f'"LokationMastrNummer" as "LocationID", ' \
                f'"NameKraftwerk" as "PowerPlantName", ' \
                f'"Nettonennleistung" as "Power", ' \
                f'"NameKraftwerksblock" as "BlockName", ' \
                f'"Kraftwerksnummer" as "PowerPlantNumber", ' \
                f'"Energietraeger" as "fuel", ' \
                f'"Laengengrad" as "lat", ' \
                f'"Breitengrad" as "lon", ' \
                f'"Einspeisungsart", ' \
                f'"AnlageIstImKombibetrieb", ' \
                f'"KwkMastrNummer" as "KWKNumber", ' \
                f'"Inbetriebnahmedatum" ' \
                f'FROM "EinheitenVerbrennung" ' \
                f'WHERE "Postleitzahl" >= {start} AND "Postleitzahl" < {end} AND "Energietraeger" = 2408' \
                f'AND "Nettonennleistung" > 100 AND "EinheitBetriebsstatus" = 35;'

        return pd.read_sql(query, self.engine)

        pass

    def get_coal_power_plants_in_area(self, area=50):
        start = area * 1000
        end = start + 1000
        # MaStR Code 2407 --> Coal
        query = f'SELECT "EinheitMastrNummer" as "ID", ' \
                f'"LokationMastrNummer" as "LocationID", ' \
                f'"NameKraftwerk" as "PowerPlantName", ' \
                f'"Nettonennleistung" as "Power", ' \
                f'"NameKraftwerksblock" as "BlockName", ' \
                f'"Kraftwerksnummer" as "PowerPlantNumber", ' \
                f'"Energietraeger" as "fuel", ' \
                f'"Laengengrad" as "lat", ' \
                f'"Breitengrad" as "lon", ' \
                f'"Einspeisungsart", ' \
                f'"AnlageIstImKombibetrieb", ' \
                f'"KwkMastrNummer" as "KWKNumber", ' \
                f'"Inbetriebnahmedatum" ' \
                f'FROM "EinheitenVerbrennung" ' \
                f'WHERE "Postleitzahl" >= {start} AND "Postleitzahl" < {end} AND "Energietraeger" = 2407' \
                f'AND "Nettonennleistung" > 100 AND "EinheitBetriebsstatus" = 35;'

        return pd.read_sql(query, self.engine)

    def get_nuclear_power_plants_in_area(self, area=50):
        pass

    def get_oil_power_plants_in_area(self, area=50):
        start = area * 1000
        end = start + 1000
        # MaStR Code 2409 --> Oil
        query = f'SELECT "EinheitMastrNummer" as "ID", ' \
                f'"LokationMastrNummer" as "LocationID", ' \
                f'"NameKraftwerk" as "PowerPlantName", ' \
                f'"Nettonennleistung" as "Power", ' \
                f'"NameKraftwerksblock" as "BlockName", ' \
                f'"Kraftwerksnummer" as "PowerPlantNumber", ' \
                f'"Energietraeger" as "fuel", ' \
                f'"Laengengrad" as "lat", ' \
                f'"Breitengrad" as "lon", ' \
                f'"Einspeisungsart", ' \
                f'"AnlageIstImKombibetrieb", ' \
                f'"KwkMastrNummer" as "KWKNumber", ' \
                f'"Inbetriebnahmedatum" ' \
                f'FROM "EinheitenVerbrennung" ' \
                f'WHERE "Postleitzahl" >= {start} AND "Postleitzahl" < {end} AND "Energietraeger" = 2409' \
                f'AND "Nettonennleistung" > 100 AND "EinheitBetriebsstatus" = 35;'

        return pd.read_sql(query, self.engine)

    def get_gas_power_plants_in_area(self, area=50):
        start = area * 1000
        end = start + 1000
        # MaStR Code 2410 --> Gas
        query = f'SELECT "EinheitMastrNummer" as "ID", ' \
                f'"LokationMastrNummer" as "LocationID", ' \
                f'"NameKraftwerk" as "PowerPlantName", ' \
                f'"Nettonennleistung" as "Power", ' \
                f'"NameKraftwerksblock" as "BlockName", ' \
                f'"Kraftwerksnummer" as "PowerPlantNumber", ' \
                f'"Energietraeger" as "fuel", ' \
                f'"Laengengrad" as "lat", ' \
                f'"Breitengrad" as "lon", ' \
                f'"Einspeisungsart", ' \
                f'"AnlageIstImKombibetrieb", ' \
                f'"KwkMastrNummer" as "KWKNumber", ' \
                f'"Inbetriebnahmedatum" ' \
                f'FROM "EinheitenVerbrennung" ' \
                f'WHERE "Postleitzahl" >= {start} AND "Postleitzahl" < {end} AND "Energietraeger" = 2410' \
                f'AND "Nettonennleistung" > 100 AND "EinheitBetriebsstatus" = 35;'

        return pd.read_sql(query, self.engine)

    def get_landfill_gas_plants_in_area(self, area=50):
        start = area * 1000
        end = start + 1000
        # MaStR Code 2411 --> landfill gas
        query = f'SELECT "EinheitMastrNummer" as "ID", ' \
                f'"LokationMastrNummer" as "LocationID", ' \
                f'"NameKraftwerk" as "PowerPlantName", ' \
                f'"Nettonennleistung" as "Power", ' \
                f'"NameKraftwerksblock" as "BlockName", ' \
                f'"Kraftwerksnummer" as "PowerPlantNumber", ' \
                f'"Energietraeger" as "fuel", ' \
                f'"Laengengrad" as "lat", ' \
                f'"Breitengrad" as "lon", ' \
                f'"Einspeisungsart", ' \
                f'"AnlageIstImKombibetrieb", ' \
                f'"KwkMastrNummer" as "KWKNumber", ' \
                f'"Inbetriebnahmedatum" ' \
                f'FROM "EinheitenVerbrennung" ' \
                f'WHERE "Postleitzahl" >= {start} AND "Postleitzahl" < {end} AND "Energietraeger" = 2411' \
                f'AND "Nettonennleistung" > 1 AND "EinheitBetriebsstatus" = 35;'

        return pd.read_sql(query, self.engine)

    def get_incineration_plants_in_area(self, area=50):
        start = area * 1000
        end = start + 1000
        # MaStR Code 2412 --> waste
        query = f'SELECT "EinheitMastrNummer" as "ID", ' \
                f'"LokationMastrNummer" as "LocationID", ' \
                f'"NameKraftwerk" as "PowerPlantName", ' \
                f'"Nettonennleistung" as "Power", ' \
                f'"NameKraftwerksblock" as "BlockName", ' \
                f'"Kraftwerksnummer" as "PowerPlantNumber", ' \
                f'"Energietraeger" as "fuel", ' \
                f'"Laengengrad" as "lat", ' \
                f'"Breitengrad" as "lon", ' \
                f'"Einspeisungsart", ' \
                f'"AnlageIstImKombibetrieb", ' \
                f'"KwkMastrNummer" as "KWKNumber", ' \
                f'"Inbetriebnahmedatum" ' \
                f'FROM "EinheitenVerbrennung" ' \
                f'WHERE "Postleitzahl" >= {start} AND "Postleitzahl" < {end} AND "Energietraeger" = 2412' \
                f'AND "Nettonennleistung" > 1 AND "EinheitBetriebsstatus" = 35;'

        return pd.read_sql(query, self.engine)

    # TODO: Check if 13 is Combined Gas and Steam Turbine
    def get_x_plants_in_area(self, area=50):
        start = area * 1000
        end = start + 1000
        # MaStR Code 2412 --> waste
        query = f'SELECT "EinheitMastrNummer" as "ID", ' \
                f'"LokationMastrNummer" as "LocationID", ' \
                f'"NameKraftwerk" as "PowerPlantName", ' \
                f'"Nettonennleistung" as "Power", ' \
                f'"NameKraftwerksblock" as "BlockName", ' \
                f'"Kraftwerksnummer" as "PowerPlantNumber", ' \
                f'"Energietraeger" as "fuel", ' \
                f'"Laengengrad" as "lat", ' \
                f'"Breitengrad" as "lon", ' \
                f'"Einspeisungsart", ' \
                f'"AnlageIstImKombibetrieb", ' \
                f'"KwkMastrNummer" as "KWKNumber", ' \
                f'"Inbetriebnahmedatum" ' \
                f'FROM "EinheitenVerbrennung" ' \
                f'WHERE "Postleitzahl" >= {start} AND "Postleitzahl" < {end} AND "Energietraeger" = 2413' \
                f'AND "Nettonennleistung" > 1 AND "EinheitBetriebsstatus" = 35;'

        return pd.read_sql(query, self.engine)

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
    pwpLignite = interface.get_lignite_power_plants_in_area(area=52)
    pwpCoal = interface.get_coal_power_plants_in_area(area=73)
    pwpOil = interface.get_oil_power_plants_in_area(area=52)
    pwpGas = interface.get_gas_power_plants_in_area(area=52)
    pwpLGas = interface.get_landfill_gas_plants_in_area(area=52)
    pwpWaste = interface.get_incineration_plants_in_area(area=52)
    pwpX = interface.get_x_plants_in_area(area=59)
    # biomass = interface.get_biomass_systems_in_area(area=20)
    #runriver = interface.get_run_river_systems_in_area(area=52)

    #pvR = interface.get_roof_top_solar_systems_in_area(area=52)
    #pcF = interface.get_free_solar_systems_in_area(area=52)
