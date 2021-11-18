from crawler.OpenDWD_Crawler import OpenDWDCrawler
import logging
import os

log = logging.getLogger('openDWD_cosmo')
log.setLevel(logging.INFO)


if __name__ == "__main__":

    try:
        crawler = OpenDWDCrawler(user=os.getenv('TIMESCALEDB_USER', 'opendata'),
                                 password=os.getenv('TIMESCALEDB_PASSWORD', 'opendata'),
                                 database=os.getenv('TIMESCALEDB_DATABASE', 'weather_cosmo'),
                                 host=os.getenv('TIMESCALEDB_HOST', '10.13.10.41'),
                                 port=int(os.getenv('TIMESCALEDB_PORT', 5432)))
        crawler.write_weather_in_timescale(start=os.getenv('START_DATE', '199501'),
                                           end=os.getenv('END_DATE', '201905'))
    except Exception as e:
        print(e)
