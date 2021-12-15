from sqlalchemy import create_engine
import pandas as pd


class MarketInterface():

    def __init__(self, ):
        self.engine = create_engine(f'postgresql://dMAS:dMAS@simulationdb/dMAS')

    def get_interval(self, start):
        start_date = pd.to_datetime(start).date()
        end_date = (start_date + pd.DateOffset(days=1)).date()
        return start_date, end_date

    def get_orders(self, date):
        return pd.read_sql("Select * from order_book", self.engine)


    def get_auction_results(self, date):
        start_date, end_date = self.get_interval(date)

        query = f"select price, volume from auction_results where time >= '{start_date}'" \
                f"and time < '{end_date}'"

        df = pd.read_sql(query, self.engine)
        df.index = pd.date_range(start=start_date, freq='h', periods=len(df))
        df.index.name = 'time'

        return df

if __name__ == "__main__":
    engine = create_engine(f'postgresql://dMAS:dMAS@localhost/dMAS')
    query = "Select * from orders"
    df = pd.read_sql(query, engine)
