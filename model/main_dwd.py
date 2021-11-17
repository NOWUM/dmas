from crawler.OpenDWD_Crawler import OpenDWDCrawler
import logging

logging.basicConfig()


if __name__ == "__main__":
    crawler = OpenDWDCrawler()
    crawler.get_data(start='199501', end='201601')
