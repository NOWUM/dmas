# third party modules
import time as tme
import pandas as pd
import numpy as np

# model modules
from aggregation.portfolio_demand import DemandPortfolio
from agents.client_Agent import agent as basicAgent


class DemAgent(basicAgent):

    def __init__(self, date, plz, mqtt_exchange, simulation_database):
        super().__init__(date, plz, 'DEM', mqtt_exchange, simulation_database)
        # Portfolio with the corresponding households, trade and industry
        self.logger.info('starting the agent')
        start_time = tme.time()
        self.portfolio = DemandPortfolio()

        # Construction of the prosumer with photovoltaic and battery
        self.logger.info('Prosumer PV-Bat added')

        # Construction consumer with photovoltaic
        self.logger.info('Consumer PV added')

        # Construction Standard Consumer H0
        self.logger.info('H0 added')

        # Construction Standard Consumer G0
        self.logger.info('G0 added')

        # Construction Standard Consumer RLM
        self.logger.info('RLM added')

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
        start_time = tme.time()                                          # performance timestamp

        weather = self.weather_forecast(self.date, mean=False)           # local weather forecast dayAhead
        self.portfolio.set_parameter(self.date, weather, dict())
        self.portfolio.build_model()

        self.performance['initModel'] = np.round(tme.time() - start_time,3)

        # Step 2: standard optimization --> returns power series in [kW]
        # -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()                                         # performance timestamp

        power_da = self.portfolio.optimize()                            # total portfolio power

        self.performance['optModel'] = np.round(tme.time() - start_time, 3)

        # Step 3: save optimization results in influxDB
        # -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()

        df = pd.DataFrame(data=dict(powerTotal=power_da/10**3, heatTotal=self.portfolio.demand['heat']/10**3,
                                    powerSolar=self.portfolio.generation['powerSolar']/10**3),
                          index=pd.date_range(start=self.date, freq='60min', periods=self.portfolio.T))

        self.connections['influxDB'].save_data(df, 'Areas', dict(typ=self.typ, agent=self.name, area=self.plz,
                                                                 timestamp='optimize_dayAhead'))

        self.performance['saveSchedule'] = np.round(tme.time() - start_time, 3)

        # Step 4: build orders from optimization results
        # -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()

        bid_orders = dict()
        for i in range(self.portfolio.T):
            bid_orders.update({str(('dem%s' % i, i, 0, str(self.name))): (3000, power_da[i]/10**3, 'x')})

        self.performance['buildOrders'] = np.round(tme.time() - start_time, 3)

        # Step 5: send orders to market resp. to mongodb
        # -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()

        self.connections['mongoDB'].set_dayAhead_orders(name=self.name, date=self.date, orders=bid_orders)

        self.performance['sendOrders'] = np.round(tme.time() - start_time, 3)

        self.logger.info('DayAhead market scheduling completed')
        print('DayAhead market scheduling completed:', self.name)

    def post_dayAhead(self):
        """Scheduling after DayAhead Market"""
        self.logger.info('After DayAhead market scheduling started')

        # Step 6: get market results and adjust generation an strategy
        # -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()

        # query the DayAhead results
        ask = self.connections['influxDB'].get_ask_da(self.date, self.name)            # volume to buy
        bid = self.connections['influxDB'].get_bid_da(self.date, self.name)            # volume to sell
        prc = self.connections['influxDB'].get_prc_da(self.date)                       # market clearing price
        profit = (ask - bid) * prc

        self.week_price_list.remember_price(prcToday=prc)

        power_da = np.asarray(self.portfolio.optimize(), np.float)                     # [kW]

        self.performance['adjustResult'] = tme.time() - start_time

        # Step 7: save adjusted results in influxdb
        # -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()

        df = pd.DataFrame(data=dict(powerTotal=power_da/10**3, heatTotal=self.portfolio.demand['heat']/10**3,
                                    powerSolar=self.portfolio.generation['powerSolar']/10**3,
                                    profit=profit))
        df.index = pd.date_range(start=self.date, freq='60min', periods=len(df))
        self.connections['influxDB'].save_data(df, 'Areas', dict(typ=self.typ, agent=self.name, area=self.plz,
                                                                 timestamp='post_dayAhead'))

        self.performance['saveResult'] = np.round(tme.time() - start_time, 3)

        self.logger.info('After DayAhead market adjustment completed')
        print('After DayAhead market adjustment completed:', self.name)
        self.logger.info('Next day scheduling started')

        # Step 8: retrain forecast methods and learning algorithm
        # -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()

        # No Price Forecast  used actually
        self.week_price_list.put_price()

        self.performance['nextDay'] = np.round(tme.time() - start_time, 3)

        df = pd.DataFrame(data=self.performance, index=[self.date])
        self.connections['influxDB'].save_data(df, 'Performance', dict(typ=self.typ, agent=self.name, area=self.plz))

        self.logger.info('Next day scheduling completed')