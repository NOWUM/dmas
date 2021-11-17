from crawler.MaStR_Crawler import init_database, get_data_from_MaStR, create_db_from_export
import time
from sqlalchemy import create_engine

if __name__ == "__main__":

    engine = create_engine('postgresql://opendata:opendata@10.13.10.41:5432')

    while True:
        init_database(connection=engine)
        mastr_data = get_data_from_MaStR()
        create_db_from_export(connection=engine)

        time.sleep(2*(60*60*24))


