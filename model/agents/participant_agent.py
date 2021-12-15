from agents.basic_Agent import BasicAgent
import pandas as pd


class ParticipantAgent(BasicAgent):

    def __init__(self, date, plz, typ, connect, infrastructure_source, infrastructure_login, *args, **kwargs):
        super().__init__(date, plz, typ, connect, infrastructure_source, infrastructure_login, *args, **kwargs)


    def set_capacities(self, portfolio):
        if isinstance(portfolio, list):
            data_frames = []
            for prt in portfolio:
                data_frames.append(pd.DataFrame(index=[pd.to_datetime(self.date)], data=prt.capacities))
            data_frame = data_frames[0]
            for df in data_frames[1:]:
                for col in df.columns:
                    data_frame[col] += df[col]
        else:
            data_frame = pd.DataFrame(index=[pd.to_datetime(self.date)], data=portfolio.capacities)

        data_frame['agent'] = self.name
        data_frame.index.name = 'time'

        data_frame.to_sql(name='capacities', con=self.simulation_database, if_exists='append')

    def set_generation(self, portfolio, step):

        if isinstance(portfolio, list):
            data_frames = []
            for prt in portfolio:
                data_frames.append(pd.DataFrame(index=pd.date_range(start=self.date, freq='h', periods=24),
                                                data=prt.generation))
            data_frame = data_frames[0]
            for df in data_frames[1:]:
                for col in df.columns:
                    data_frame[col] += df[col]
        else:
            data_frame = pd.DataFrame(index=pd.date_range(start=self.date, freq='h', periods=24),
                                      data=portfolio.generation)

        data_frame['agent'] = self.name
        data_frame.index.name = 'time'
        data_frame['step'] = step

        data_frame.to_sql(name='generation', con=self.simulation_database, if_exists='append')


    def set_demand(self, portfolio, step):

        if isinstance(portfolio, list):
            data_frames = []
            for prt in portfolio:
                data_frames.append(pd.DataFrame(index=pd.date_range(start=self.date, freq='h', periods=24),
                                                data=prt.demand))
            data_frame = data_frames[0]
            for df in data_frames[1:]:
                for col in df.columns:
                    data_frame[col] += df[col]
        else:
            data_frame = pd.DataFrame(index=pd.date_range(start=self.date, freq='h', periods=24),
                                      data=portfolio.demand)

        data_frame['agent'] = self.name
        data_frame.index.name = 'time'
        data_frame['step'] = step

        data_frame.to_sql(name='demand', con=self.simulation_database, if_exists='append')

    def set_order_book(self, order_book):
        order_book.to_sql('order_book', con=self.simulation_database, if_exists='append')