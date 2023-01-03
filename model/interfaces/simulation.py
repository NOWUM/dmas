from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
import pandas as pd
import numpy as np
import logging
from datetime import timedelta as td


def get_interval(start):
    start_date = pd.to_datetime(start).date()
    end_date = (start_date + pd.DateOffset(days=1)).date()
    return start_date, end_date


def merge_portfolio(portfolio, type, date):
    data_frames = []
    for prt in portfolio:
        if type == 'capacities':
            data_frames.append(pd.DataFrame(index=[pd.to_datetime(date)], data=prt.capacities))
        if type == 'generation':
            data_frames.append(pd.DataFrame(index=pd.date_range(start=date, freq='h',
                                                                periods=len(prt.generation['total'])),
                                            data=prt.generation))
        if type == 'demand':
            data_frames.append(pd.DataFrame(index=pd.date_range(start=date, freq='h',
                                                                periods=len(prt.demand['power'])),
                                            data=prt.demand))
    data_frame = data_frames[0]
    for df in data_frames[1:]:
        for col in df.columns:
            data_frame[col] += df[col]
    return data_frame


class SimulationInterface:

    def __init__(self, name, simulation_db_uri):
        self.database = create_engine(simulation_db_uri,
                                      connect_args={"application_name": name})
        self.name = name
        self.logger = logging.getLogger('simulation')

    def initialize_tables(self):
        with self.database.begin() as connection:
            # initialize tables for orders and market
            # hourly orders
            query = '''
              DROP TABLE IF EXISTS hourly_orders;
              DROP TABLE IF EXISTS linked_orders;
              DROP TABLE IF EXISTS exclusive_orders;
              DROP TABLE IF EXISTS capacities;
              DROP TABLE IF EXISTS demand;
              DROP TABLE IF EXISTS generation;
              DROP TABLE IF EXISTS orders;
              DROP TABLE IF EXISTS auction_results;
              DROP TABLE IF EXISTS hourly_results;
              DROP TABLE IF EXISTS linked_results;
              DROP TABLE IF EXISTS exclusive_results;
            '''
            connection.execute(query)

            query = '''CREATE TABLE hourly_orders (hour bigint, block_id bigint , order_id bigint, name text,
                                                price double precision, volume double precision, type text)'''
            connection.execute(query)
            connection.execute('ALTER TABLE "hourly_orders" ADD PRIMARY KEY ("block_id", "hour", "order_id", "name")')
            # linked block orders
            query = '''CREATE TABLE linked_orders (block_id bigint, hour bigint, order_id bigint, name text, price double precision,
                                                volume double precision, link bigint, type text)'''
            connection.execute(query)
            connection.execute('ALTER TABLE "linked_orders" ADD PRIMARY KEY ("block_id", "hour", "order_id", "name");')
            # exclusive block orders
            query = '''CREATE TABLE exclusive_orders (block_id bigint, hour bigint, name text, price double precision,
                                                    volume double precision)'''
            connection.execute(query)
            connection.execute('ALTER TABLE "exclusive_orders" ADD PRIMARY KEY ("block_id", "hour", "name");')

            # Generation and Demand Data
            # installed capacities
            query = '''CREATE TABLE capacities ("time" timestamp without time zone, bio double precision,
                                                coal double precision, gas double precision, lignite double precision,
                                                nuclear double precision, solar double precision, water double precision,
                                                wind double precision, storage double precision, agent text,
                                                area text)'''
            connection.execute(query)
            connection.execute('ALTER TABLE "capacities" ADD PRIMARY KEY ("time", "agent");')
            # total demand of each agent
            query = '''CREATE TABLE demand ("time" timestamp without time zone, power double precision,
                                            heat double precision, step text, agent text, area text)'''
            connection.execute(query)
            connection.execute('ALTER TABLE "demand" ADD PRIMARY KEY ("time", "step", "agent");')
            # total generation of each agent
            query = '''CREATE TABLE generation ("time" timestamp without time zone, total double precision,
                                                solar double precision, wind double precision, water double precision,
                                                bio double precision, lignite double precision, coal double precision,
                                                gas double precision, nuclear double precision,
                                                allocation double precision, step text,
                                                agent text, area text)'''
            connection.execute(query)
            connection.execute(f'ALTER TABLE "generation" ADD PRIMARY KEY ("time", "step", "agent");')

            query = '''CREATE TABLE auction_results ("time" timestamp without time zone, price double precision,
                                                    volume double precision)'''
            connection.execute(query)
            connection.execute(f'ALTER TABLE "auction_results" ADD PRIMARY KEY ("time");')

            # hourly orders
            query = '''CREATE TABLE hourly_results (hour bigint, block_id bigint , order_id bigint, name text,
                                                    price double precision, volume double precision, type text)'''
            connection.execute(query)
            connection.execute('ALTER TABLE "hourly_results" ADD PRIMARY KEY ("block_id", "hour", "order_id", "name")')
            # linked block orders
            query = '''CREATE TABLE linked_results(block_id bigint, hour bigint, order_id bigint, name text, price double precision,
                                                    volume double precision, link bigint, type text)'''
            connection.execute(query)
            connection.execute('ALTER TABLE "linked_results" ADD PRIMARY KEY ("block_id", "hour", "order_id", "name");')
            # exclusive block orders
            query = '''CREATE TABLE exclusive_results (block_id bigint, hour bigint, name text, price double precision,
                                                        volume double precision)'''
            connection.execute(query)
            connection.execute('ALTER TABLE "exclusive_results" ADD PRIMARY KEY ("block_id", "hour", "name");')

            query = '''CREATE TABLE orders ("time" timestamp without time zone, total double precision,
                                            volume double precision, price double precision, block_id integer,
                                            agent text, area text)'''
            connection.execute(query)
            connection.execute(f'ALTER TABLE orders ADD PRIMARY KEY ("time","agent","block_id");')

        hypertables = ['orders', 'auction_results', 'capacities', 'generation', 'demand']

        for hypertable in hypertables:
            query_create_hypertable = f"SELECT create_hypertable('{hypertable}', 'time', if_not_exists => TRUE, migrate_data => TRUE);"
            with self.database.connect() as conn, conn.begin():
                print(query_create_hypertable)
                conn.execute(query_create_hypertable)

    def reset_order_book(self):
        with self.database.begin() as connection:
            connection.execute("DELETE FROM hourly_orders")
            connection.execute("DELETE FROM linked_orders")
            connection.execute("DELETE FROM exclusive_orders")

    def set_capacities(self, portfolio, area, date):
        if isinstance(portfolio, list):
            data_frame = merge_portfolio(portfolio, type='capacities', date=date)
        else:
            data_frame = pd.DataFrame(index=[pd.to_datetime(date)], data=portfolio.capacities)

        data_frame['agent'] = self.name
        data_frame['area'] = area
        data_frame.index.name = 'time'

        try:
            data_frame.to_sql(name='capacities', con=self.database, if_exists='append')
        except IntegrityError:
            self.logger.error(f'capacities already exist for {area} and {date} - ignoring')

    def get_global_capacities(self, date):
        query = f"SELECT sum(bio) as bio, " \
                f"sum(water) as water, " \
                f"sum(wind) as wind, " \
                f"sum(solar) as solar, " \
                f"sum(nuclear) as nuclear, " \
                f"sum(lignite) as lignite, " \
                f"sum(coal) as coal, " \
                f"sum(gas) as gas " \
                f"FROM capacities WHERE time='{date.isoformat()}'"
        return pd.read_sql(query, self.database)

    def set_generation(self, portfolio, step, area, date):
        if isinstance(portfolio, list):
            data_frame = merge_portfolio(portfolio, type='generation', date=date)
        else:
            data_frame = pd.DataFrame(index=pd.date_range(start=date, freq='h', periods=24),
                                      data=portfolio.generation)

        data_frame['agent'] = self.name
        data_frame['area'] = area
        data_frame['step'] = step
        data_frame.index.name = 'time'

        # generation in kW
        try:
            data_frame.to_sql(name='generation', con=self.database, if_exists='append')
        except IntegrityError:
            self.logger.error(f'generation already exists for {area} and {date} - ignoring')

    def get_generation(self, date, agent=None, step='optimize_dayAhead'):
        '''optimize_dayAhead for planned, results_dayAhead for final'''
        try:
            query = f"""
select time,
sum(total) as total,
sum(solar) as solar,
sum(wind) as wind,
sum(water) as water,
sum(bio) as bio,
sum(lignite) as lignite,
sum(coal) as coal,
sum(gas) as gas,
sum(nuclear) as nuclear,
sum(storage) as storage
from generation where step ='{step}'"""
            if agent:
                query += f"and agent = {agent}'"
            else:
                query += "and agent not like '%%dem%%'"
            query += ' group by time order by time'
            df = pd.read_sql(query, self.database, index_col="time")

        except Exception as e:
            self.logger.exception(f'error getting generation: {e}')
            df = pd.DataFrame(data=dict(total=np.zeros(24),
                                        water=np.zeros(24),
                                        bio=np.zeros(24),
                                        wind=np.zeros(24),
                                        solar=np.zeros(24),
                                        lignite=np.zeros(24),
                                        coal=np.zeros(24),
                                        gas=np.zeros(24),
                                        nuclear=np.zeros(24)),
                              )
        return df

    def set_demand(self, portfolio, step, area, date):

        if isinstance(portfolio, list):
            data_frame = merge_portfolio(portfolio, type='demand', date=date)
        else:
            data_frame = pd.DataFrame(index=pd.date_range(start=date, freq='h', periods=24),
                                      data=portfolio.demand)

        data_frame['agent'] = self.name
        data_frame['area'] = area
        data_frame.index.name = 'time'
        data_frame['step'] = step

        try:
            data_frame.to_sql(name='demand', con=self.database, if_exists='append')
        except IntegrityError:
            self.logger.error(f'demand already exists for {area} and {date} - ignoring')

    # hourly orders
    def set_hourly_orders(self, order_book):
        try:
            order_book.to_sql('hourly_orders', con=self.database, if_exists='append')
        except IntegrityError:
            self.logger.warning(f'hourly_orders already exists - ignoring')

    def get_hourly_orders(self):
        df = pd.read_sql("Select * from hourly_orders", self.database)
        df = df.set_index(['block_id', 'hour', 'name'])
        return df

    def get_hourly_result(self, name):
        df = pd.read_sql(f"Select hour, sum(volume) as volume from hourly_results "
                         f"where name = '{name}' group by hour",
                                          self.database)
        return df

    def get_bid_result(self, name):
        df = pd.read_sql(f"Select hour, sum(volume) as volume from bid_results "
                         f"where name = '{name}' group by hour",
                         self.database)
        return df

    # linked orders
    def set_linked_orders(self, order_book):
        try:
            order_book.to_sql('linked_orders', con=self.database, if_exists='append')
        except IntegrityError:
            self.logger.error(f'orders already exists for {self.name} - ignoring')

    def get_linked_orders(self):
        df = pd.read_sql("Select * from linked_orders", self.database)
        df = df.set_index(['block_id', 'hour', 'name'])
        return df

    def get_linked_result(self, names: list[str]):
        in_string = "','".join(names)
        df = pd.read_sql(f"Select hour, sum(volume) as volume, name from linked_results "
                         f"where name in ('{in_string}') group by hour, name",
                         self.database)
        return df

    # exclusive orders
    def set_exclusive_orders(self, order_book):
        try:
            order_book.to_sql('exclusive_orders', con=self.database, if_exists='append')
        except IntegrityError as e:
            print(e)
            self.logger.error(f'exclusive orders already exists for {self.name} - ignoring')

    def get_exclusive_orders(self):
        df = pd.read_sql("Select * from exclusive_orders", self.database)
        df = df.set_index(['block_id', 'hour', 'name'])
        return df

    def get_exclusive_result(self, names: list[str]):
        in_string = "','".join(names)
        df = pd.read_sql(f"Select hour, sum(volume) as volume, name from exclusive_results "
                         f"where name in ('{in_string}') group by hour, name",
                         self.database)
        return df

    # market result
    def set_market_results(self, results):
        for key, value in results.items():
            value.to_sql(key, self.database, if_exists='replace')

    def set_auction_results(self, auction_results: pd.DataFrame, merit_order: dict = None):
        date = auction_results.index[0]
        auction_results.to_sql('auction_results', self.database, if_exists='append')
        for hour_values in merit_order.values():
            data = pd.DataFrame.from_dict(hour_values)
            ask = data.loc[data['type'] == 'ask'].sort_values('price')
            ask['volume'] = ask['volume'].cumsum()
            bid = data.loc[data['type'] == 'bid'].sort_values('price', ascending=False)
            bid['volume'] = bid['volume'].sum()
            bid['price'] = np.linspace(start=3, stop=-0.5, num=len(bid['price'].values))

            data = pd.concat([ask, bid], axis=0)

            data['time'] = date
            data['hour'] = date.hour
            data.to_sql('merit_order', self.database, if_exists='append', index=False)
            date += td(hours=1)

    def get_auction_results_range(self, start, end):
        query = f"select time, price, volume from auction_results where time >= '{start}'" \
                f"and time < '{end}'"
        return pd.read_sql(query, self.database, index_col="time", parse_dates=["time"])

    def get_auction_results(self, date):
        start_date, end_date = get_interval(date)
        df = self.get_auction_results_range(start_date, end_date)
        df.index = pd.date_range(start=start_date, freq='h', periods=len(df))
        df.index.name = 'time'
        return df

    def set_orders(self, order_book, date, area):

        mapping = dict(price='mean', volume='sum')
        order_book = order_book.groupby(order_book.index.get_level_values('hour')).aggregate(mapping)
        order_book.index = [date + td(hours=int(hour)) for hour in order_book.index]
        order_book.index.name = 'time'
        order_book['agent'] = self.name
        order_book['area'] = area

        try:
            order_book.to_sql(name='orders', con=self.database, if_exists='append')
        except IntegrityError:
            self.logger.error(f'orders already exists for {self.name} and {date} - ignoring')

    def set_cash_flow(self, portfolio, area, date):

        data_frame = pd.DataFrame(index=pd.date_range(start=date, freq='h', periods=24),
                                  data=portfolio.cash_flow)

        data_frame['agent'] = self.name
        data_frame['area'] = area
        data_frame.index.name = 'time'

        try:
            data_frame.to_sql(name='cash_flows', con=self.database, if_exists='append')
        except IntegrityError:
            self.logger.error(f'orders already exists for {self.name} and {date} - ignoring')

    def get_sim_start_and_end(self):
        start = self.database.execute('select "time" from  capacities order by "time" asc limit 1').fetchall()[0][0]
        end = self.database.execute('select "time" from  capacities order by "time" desc limit 1').fetchall()[0][0]
        return start, end
