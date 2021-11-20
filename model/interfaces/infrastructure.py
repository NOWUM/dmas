from sqlalchemy import create_engine
import pandas as pd


class Infrastructure:

    def __init__(self, user='opendata', password='opendata', database='mastr', host='10.13.10.41', port=5432):

        self.engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

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
                f'"Hauptausrichtung" as azimuth, '  \
                f'"Leistungsbegrenzung", ' \
                f'"Einspeisungsart", ' \
                f'"FernsteuerbarkeitNb", ' \
                f'"HauptausrichtungNeigungswinkel", ' \
                f'"Inbetriebnahmedatum" ' \
                f'FROM "EinheitenSolar" ' \
                f' INNER JOIN "AnlagenEegSolar" ON "EinheitMastrNummer" = "VerknuepfteEinheitenMastrNummern" ' \
                f'WHERE "Postleitzahl" >= {start} AND "Postleitzahl" < {end} AND "Lage" = 853 ' \
                f'AND "EinheitBetriebsstatus" = 35;'
        # f'"EegMastrNummer" as EEGNr, ' \
        df = pd.read_sql(query, self.engine).iloc[:-2]
        print(df.columns)
        eeg = []
        # for value in df['id'].to_numpy():
        #     try:
        #         query = f'SELECT "InanspruchnahmeZahlungNachEeg" as EEG ' \
        #                 f'FROM "AnlagenEegSolar" ' \
        #                 f'WHERE "VerknuepfteEinheitenMastrNummern" = "{value}"'
        #         res = self.engine.execute(query)
        #         data = res.fetchall()[0][0]
        #         eeg.append(data)
        #         print('--------------------> added')
        #     except Exception as e:
        #         print(e)
        #         eeg.append(None)
        # df['eeg'] = eeg

        # df['azimuth'] = 180
        # df['tilt'] = 30

        return df

    def get_biomass_systems_in_area(self, area=50):
        pass

    def get_run_river_systems_in_area(self, area=50):
        pass


if __name__ == "__main__":

    interface = Infrastructure()
    pvR = interface.get_roof_top_solar_systems_in_area(area=52)
    pcF = interface.get_free_solar_systems_in_area(area=52)
