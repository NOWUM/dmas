from crawler.OpenDWD_Crawler import OpenDWDCrawler
import logging

logging.basicConfig()


if __name__ == "__main__":
    crawler = OpenDWDCrawler()
    crawler.write_weather_in_timescale(start='199501', end='201601')
