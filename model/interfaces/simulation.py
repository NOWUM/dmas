from sqlalchemy import create_engine
import requests
import pandas as pd


def get_interval(start):
    start_date = pd.to_datetime(start).date()
    end_date = (start_date + pd.DateOffset(days=1)).date()
    return start_date, end_date


class SimulationInterface:

    def __init__(self, name, simulation_data_server, simulation_data_credential,
                 structure_database, mqtt_server):
        self.database = create_engine(f'postgresql://{simulation_data_credential}@{simulation_data_server}/'
                                      f'{structure_database}',
                                      connect_args={"application_name": name})
        self.name = name
        self.date = None
        self.mqtt_server = mqtt_server

    def initial_tables(self):

        # initialize tables for orders and market
        # hourly orders
        query = '''CREATE TABLE hourly_orders (hour bigint, order_id bigint, name text, price double precision,
                                               volume double precision, type text)'''
        self.database.execute(query)
        self.database.execute('ALTER TABLE "hourly_orders" ADD PRIMARY KEY ("hour", "order_id", "name");')
        # linked block orders
        query = '''CREATE TABLE linked_orders (block_id bigint, hour bigint, order_id bigint, name text, price double precision,
                                               volume double precision, link bigint, type text)'''
        self.database.execute(query)
        self.database.execute('ALTER TABLE "linked_orders" ADD PRIMARY KEY ("block_id", "hour", "order_id", "name");')
        # exclusive block orders
        query = '''CREATE TABLE exclusive_orders (block_id bigint, hour bigint, name text, price double precision,
                                                  volume double precision)'''
        self.database.execute(query)
        self.database.execute('ALTER TABLE "exclusive_orders" ADD PRIMARY KEY ("block_id", "hour", "name");')

        # Generation and Demand Data
        # installed capacities
        query = '''CREATE TABLE capacities ("time" timestamp without time zone, bio double precision,
                                            coal double precision, gas double precision, lignite double precision,
                                            nuclear double precision, solar double precision, water double precision,
                                            wind double precision, storage double precision, agent text)'''
        self.database.execute(query)
        self.database.execute('ALTER TABLE "capacities" ADD PRIMARY KEY ("time", "agent");')
        # total demand of each agent
        query = '''CREATE TABLE demand ("time" timestamp without time zone, power double precision,
                                        heat double precision, step text, agent text)'''
        self.database.execute(query)
        self.database.execute('ALTER TABLE "demand" ADD PRIMARY KEY ("time", "step", "agent");')
        # total generation of each agent
        query = '''CREATE TABLE generation ("time" timestamp without time zone, total double precision,
                                            solar double precision, wind double precision, water double precision,
                                            bio double precision, lignite double precision, coal double precision,
                                            gas double precision, nuclear double precision, step text,
                                            agent text)'''
        self.database.execute(query)
        self.database.execute(f'ALTER TABLE "generation" ADD PRIMARY KEY ("time", "step", "agent");')

        query = '''CREATE TABLE auction_results ("time" timestamp without time zone, price double precision,
                                                 volume double precision)'''
        self.database.execute(query)
        self.database.execute(f'ALTER TABLE "auction_results" ADD PRIMARY KEY ("time");')

        query = '''CREATE TABLE market_results (block_id bigint, hour bigint, order_id bigint, name text, price double precision,
                                                volume double precision, link bigint, type text)'''
        self.database.execute(query)
        self.database.execute('ALTER TABLE "market_results" ADD PRIMARY KEY ("block_id", "hour", "order_id", "name");')

    def reset_order_book(self):
        self.database.execute("DELETE FROM hourly_orders")
        self.database.execute("DELETE FROM linked_orders")
        self.database.execute("DELETE FROM exclusive_orders")

    def merge_portfolio(self, portfolio, type):
        data_frames = []
        for prt in portfolio:
            if type == 'capacities':
                data_frames.append(pd.DataFrame(index=[pd.to_datetime(self.date)], data=prt.capacities))
            if type == 'generation':
                data_frames.append(pd.DataFrame(index=[pd.to_datetime(self.date)], data=prt.generation))
            if type == 'demand':
                data_frames.append(pd.DataFrame(index=[pd.to_datetime(self.date)], data=prt.demand))

        data_frame = data_frames[0]
        for df in data_frames[1:]:
            for col in df.columns:
                data_frame[col] += df[col]

        return data_frame

    def set_capacities(self, portfolio, area):
        if isinstance(portfolio, list):
            data_frame = self.merge_portfolio(portfolio, type='capacities')
        else:
            data_frame = pd.DataFrame(index=[pd.to_datetime(self.date)], data=portfolio.capacities)

        data_frame['agent'] = self.name
        data_frame['area'] = area
        data_frame.index.name = 'time'

        data_frame.to_sql(name='capacities', con=self.database, if_exists='append')

    def set_generation(self, portfolio, step, area):
        if isinstance(portfolio, list):
            data_frame = self.merge_portfolio(portfolio, type='generation')
        else:
            data_frame = pd.DataFrame(index=pd.date_range(start=self.date, freq='h', periods=24),
                                      data=portfolio.generation)

        data_frame['agent'] = self.name
        data_frame['area'] = area
        data_frame['step'] = step
        data_frame.index.name = 'time'

        data_frame.to_sql(name='generation', con=self.database, if_exists='append')

    def set_demand(self, portfolio, step, area):

        if isinstance(portfolio, list):
            data_frame = self.merge_portfolio(portfolio, type='demand')
        else:
            data_frame = pd.DataFrame(index=pd.date_range(start=self.date, freq='h', periods=24),
                                      data=portfolio.demand)

        data_frame['agent'] = self.name
        data_frame.index.name = 'time'
        data_frame['step'] = step

        data_frame.to_sql(name='demand', con=self.database, if_exists='append')

    def set_order_book(self, order_book):
        order_book.to_sql('order_book', con=self.database, if_exists='append')

    def set_market_results(self, market_results):
        market_results.to_sql('market_results', self.database, if_exists='replace')

    def set_auction_results(self, auction_results):
        auction_results.to_sql('auction_results', self.database, if_exists='append')

    def get_orders(self, date):
        return pd.read_sql("Select * from order_book", self.database)

    def get_auction_results(self):
        start_date, end_date = get_interval(self.date)

        query = f"select price, volume from auction_results where time >= '{start_date}'" \
                f"and time < '{end_date}'"

        df = pd.read_sql(query, self.database)
        df.index = pd.date_range(start=start_date, freq='h', periods=len(df))
        df.index.name = 'time'

        return df

    def get_hourly_orders(self):
        df = pd.read_sql("Select * from hourly_orders", self.database)
        df = df.set_index(['block_id', 'hour', 'order_id', 'name'])
        return df

    def get_linked_orders(self):
        df = pd.read_sql("Select * from linked_orders", self.database)
        df = df.set_index(['block_id', 'hour', 'order_id', 'name'])
        return df

    def get_exclusive_orders(self):
        df = pd.read_sql("Select * from exclusive_orders", self.database)
        df = df.set_index(['block_id', 'hour', 'name'])
        return df

    def get_agents(self):
        headers = {'content-type': 'application/json', }
        response = requests.get(f'http://{self.mqtt_server}:15672/api/queues', headers=headers, auth=('guest', 'guest'))
        agents = response.json()
        return [agent['name'] for agent in agents if agent['name'][:3]
                in ['dem', 'res', 'str', 'pwp']]