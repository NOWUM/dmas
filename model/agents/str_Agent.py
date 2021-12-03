# third party modules
import time as tme
import pandas as pd
import numpy as np
from scipy.stats import norm
from collections import deque

# model modules
from aggregation.portfolio_storage import StrPort
from agents.client_Agent import agent as basicAgent


class StrAgent(basicAgent):

    def __init__(self, date, plz, agent_type, mqtt_exchange, connect,  infrastructure_source, infrastructure_login):
        super().__init__(date, plz, agent_type, mqtt_exchange, connect, infrastructure_source, infrastructure_login)
        # Development of the portfolio with the corresponding power plants and storages
        self.logger.info('starting the agent')
        start_time = tme.time()
        self.portfolio = StrPort(T=24)

        self.max_volume = 0
        # Construction storages
        storages = self.infrastructure_interface.get_water_storage_systems(area=plz)
        if storages is not None:
            for _, data in storages.iterrows():
                    self.portfolio.add_energy_system(data.to_dict())

        self.logger.info('Storages added')

        mcp = [37.70, 35.30, 33.90, 33.01, 33.27, 35.78, 43.17, 50.21, 52.89, 51.18, 48.24, 46.72, 44.23,
               42.29, 41.60, 43.12, 45.37, 50.95, 55.12, 56.34, 52.70, 48.20, 45.69, 40.25]

        self.price_history = deque(maxlen=1000)
        self.price_history.append(np.asarray(mcp).reshape((1, -1)))

        self.offset_ask = 0
        self.offset_bid = 0
        self.start_gap = 0

        self.q_ask = [0.55, 0.60, 0.65, 0.75, 0.85, 0.95]
        self.q_bid = [0.45, 0.40, 0.35, 0.25, 0.15, 0.05]

        df = pd.DataFrame(index=[pd.to_datetime(self.date)], data=self.portfolio.capacities)
        df['agent'] = self.name
        df.to_sql(name='installed capacities', con=self.simulation_database)

        self.logger.info('setup of the agent completed in %s' % (tme.time() - start_time))

    def callback(self, ch, method, properties, body):
        super().callback(ch, method, properties, body)

        message = body.decode("utf-8")
        self.date = pd.to_datetime(message.split(' ')[1])
        # Call DayAhead Optimization
        # -----------------------------------------------------------------------------------------------------------
        if 'opt_dayAhead' in message:
            try:
                self.optimize_dayAhead()
            except:
                self.logger.exception('Error during day Ahead optimization')

        # Call DayAhead Result
        # -----------------------------------------------------------------------------------------------------------
        if 'result_dayAhead' in message:
            try:
                self.post_dayAhead()
            except:
                self.logger.exception('Error in After day Ahead process')

    def optimize_dayAhead(self):
        """scheduling for the DayAhead market"""
        self.logger.info('DayAhead market scheduling started')

        # Step 1: forecast input data and init the model for the coming day
        # -------------------------------------------------------------------------------------------------------------

        prices = self.price_forecast(self.date)                        # price forecast dayAhead
        self.portfolio.set_parameter(self.date, dict(), prices)
        self.portfolio.build_model()
        self.portfolio.optimize()                                      # optimize portfolio

        # calculate base price --> mean for 24h
        base_prc = np.mean(prices['power'])
        # calculate standard deviation for the price history
        std_prc = np.sqrt(np.var(np.asarray(self.price_history)))

        order_book = {}

        for key, value in self.portfolio.energy_systems.items():
            efficiency = value['eta+'] * value['eta-']

            min_ask_prc = base_prc * (1.5 - efficiency/2)
            max_bid_prc = base_prc * (0.5 + efficiency/2)

            prc_ask = [max(norm.ppf(element, base_prc, std_prc), min_ask_prc) for element in self.q_ask]
            prc_ask.insert(0, min_ask_prc)

            prc_bid = [min(norm.ppf(element, base_prc, std_prc), max_bid_prc) for element in self.q_bid]
            prc_bid.insert(0, max_bid_prc)

            block_number = 0

            for i in self.portfolio.t:
                m_ask = (2 * self.offset_ask) / 6
                vol_ask = 1/7 - self.offset_ask

                m_bid= (2 * self.offset_bid) / 6
                vol_bid = 1/7 - self.offset_bid

                for k in range(7):

                    price = np.round(prc_ask[k],2) + self.start_gap
                    volume = np.round(vol_ask * value['P+_Max'], 2)
                    order_book.update({str(('gen%s' % block_number, i, k, self.name)): (price, volume, 'x')})
                    vol_ask += m_ask

                    price = np.round(prc_bid[k],2) + self.start_gap
                    volume = np.round(vol_bid * value['P+_Max'], 2)
                    order_book.update({str(('dem%s' % block_number, i, k, self.name)): (price, volume, 'x')})
                    vol_bid += m_bid

                block_number += 1

        # Step 5: send orders to market resp. to mongodb
        # -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()

        self.connections['mongoDB'].set_dayAhead_orders(name=self.name, date=self.date, orders=order_book)

        self.performance['sendOrders'] = tme.time() - start_time

        self.logger.info('DayAhead market scheduling completed')
        print('DayAhead market scheduling completed:', self.name)

    def post_dayAhead(self):
        """Scheduling after DayAhead Market"""
        self.logger.info('After DayAhead market scheduling started')

        # Step 6: get market results and adjust generation
        # -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()

        # query the DayAhead results
        ask = self.connections['influxDB'].get_ask_da(self.date, self.name)            # volume to buy
        bid = self.connections['influxDB'].get_bid_da(self.date, self.name)            # volume to sell
        prc = self.connections['influxDB'].get_prc_da(self.date)                       # market clearing price
        profit = (ask - bid) * prc

        gap  = np.mean(prc) - np.mean(self.price_forecast(self.date)['power'])

        self.week_price_list.remember_price(prcToday=prc)

        if np.abs(gap) > 2:
            self.start_gap = gap
        else:
            v0 = self.portfolio.volume[0]
            if v0 + np.sum(bid) * 0.9 - np.sum(ask) / 0.9 < 0:
                self.offset_ask += 0.01
                self.offset_ask = min(self.offset_ask, 1/7)
            if v0 + np.sum(bid) * 0.9 - np.sum(ask) / 0.9 > self.max_volume:
                self.offset_bid += 0.01
                self.offset_bid = min(self.offset_bid, 1/7)

        # adjust power generation
        self.portfolio.build_model(response=ask - bid)
        self.portfolio.optimize()
        volume = self.portfolio.volume
        self.performance['adjustResult'] = tme.time() - start_time

        self.base_price = np.concatenate((self.base_price, prc.reshape((1, 24))), axis=0)

        # Step 7: save adjusted results in influxdb
        # -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()

        df = pd.concat([pd.DataFrame.from_dict(self.portfolio.generation),
                        pd.DataFrame(data=dict(profit=profit, volume=volume))], axis=1)
        df.index = pd.date_range(start=self.date, freq='60min', periods=len(df))
        self.connections['influxDB'].save_data(df, 'Areas', dict(typ=self.typ, agent=self.name, area=self.plz,
                                                                 timestamp='post_dayAhead'))

        self.performance['saveResult'] = tme.time() - start_time

        self.logger.info('After DayAhead market adjustment completed')
        print('After DayAhead market adjustment completed:', self.name)
        self.logger.info('Next day scheduling started')

        # Step 8: retrain forecast methods and learning algorithm
        # -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()

        # collect data an retrain forecast method
        dem = self.connections['influxDB'].get_dem(self.date)  # demand germany [MW]
        weather = self.forecasts['weather'].mean_weather  # weather data
        prc_1 = self.week_price_list.get_price_yesterday()  # mcp yesterday [€/MWh]
        prc_7 = self.week_price_list.get_price_week_before()  # mcp week before [€/MWh]
        for key, method in self.forecasts.items():
            method.collect_data(date=self.date, dem=dem, prc=prc[:24], prc_1=prc_1, prc_7=prc_7, weather=weather)
            method.counter += 1
            if method.counter >= method.collect:  # retrain forecast method
                method.fit_function()
                method.counter = 0

        self.week_price_list.put_price()

        df = pd.DataFrame(index=[pd.to_datetime(self.date)], data=self.portfolio.capacities)
        self.connections['influxDB'].save_data(df, 'Areas', dict(typ=self.typ, agent=self.name, area=self.plz))

        self.performance['nextDay'] = tme.time() - start_time

        #df = pd.DataFrame(data=self.performance, index=[self.date])
        #self.connections['influxDB'].save_data(df, 'Performance', dict(typ=self.typ, agent=self.name, area=self.plz))

        self.logger.info('Next day scheduling completed')