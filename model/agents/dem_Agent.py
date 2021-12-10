# third party modules
import time as time
import pandas as pd
import numpy as np
from tqdm import tqdm

# model modules
from forecasts.weather import WeatherForecast
from forecasts.price import PriceForecast
from aggregation.portfolio_demand import DemandPortfolio
from agents.basic_Agent import BasicAgent


class DemAgent(BasicAgent):

    def __init__(self, date, plz, agent_type, connect,  infrastructure_source, infrastructure_login, *args, **kwargs):
        super().__init__(date, plz, agent_type, connect, infrastructure_source, infrastructure_login)
        # Portfolio with the corresponding households, trade and industry
        self.logger.info('starting the agent')
        start_time = time.time()
        self.portfolio = DemandPortfolio()
        self.weather_forecast = WeatherForecast(position=dict(lat=self.latitude, lon=self.longitude))
        self.price_forecast = PriceForecast(position=dict(lat=self.latitude, lon=self.longitude))

        demand = 0
        # Construction of the prosumer with photovoltaic and battery
        bats = self.infrastructure_interface.get_solar_storage_systems_in_area(area=plz)
        bats['type'] = 'battery'
        for system in tqdm(bats.to_dict(orient='records')):
            self.portfolio.add_energy_system(system)
            demand += system['demandP'] / 10**9
        self.logger.info('Prosumer Photovoltaic and Battery added')

        # Construction consumer with photovoltaic
        pvs = self.infrastructure_interface.get_solar_systems_in_area(area=plz, solar_type='roof_top')
        pvs = pvs[pvs['ownConsumption'] == 1]
        pvs['maxPower'] *= 1000
        pvs['type'] = 'solar'
        for system in tqdm(pvs.to_dict(orient='records')):
            self.portfolio.add_energy_system(system)
            demand += system['demandP'] / 10**9
        self.logger.info('Prosumer Photovoltaic added')

        total_demand, household, industry_business = self.infrastructure_interface.get_demand_in_area(area=plz)
        household_demand = (total_demand * household - demand) * 10**9
        business_demand = total_demand * industry_business * 0.5 * 10**9
        industry_demand = business_demand

        # Construction Standard Consumer H0
        self.portfolio.add_energy_system({'unitID': 'household', 'demandP': household_demand, 'type': 'household'})
        self.logger.info('H0 added')

        # Construction Standard Consumer G0
        self.portfolio.add_energy_system({'unitID': 'business', 'demandP': business_demand, 'type': 'business'})
        self.logger.info('G0 added')

        # Construction Standard Consumer RLM
        self.portfolio.add_energy_system({'unitID': 'industry', 'demandP': industry_demand, 'type': 'industry'})
        self.logger.info('RLM added')

        df = pd.DataFrame(index=[pd.to_datetime(self.date)], data=self.portfolio.capacities)
        df['agent'] = self.name
        # df.to_sql(name='installed capacities', con=self.simulation_database, if_exists='replace')

        self.logger.info(f'setup of the agent completed in {np.round(time.time() - start_time,2)} seconds')

    def callback(self, ch, method, properties, body):
        super().callback(ch, method, properties, body)

        message = body.decode("utf-8")
        self.date = pd.to_datetime(message.split(' ')[1])

        if 'opt_dayAhead' in message:
            self.optimize_day_ahead()
        if 'result_dayAhead' in message:
            self.post_dayAhead()

    def optimize_day_ahead(self):
        """scheduling for the DayAhead market"""
        self.logger.info('Starting day ahead optimization')
        start_time = time.time()

        # Step 1: forecast data data and init the model for the coming day
        weather = self.weather_forecast.forecast_for_area(self.date, int(self.plz/10))
        prices = self.price_forecast.forecast(self.date)

        self.portfolio.set_parameter(self.date, weather.copy(), prices.copy())
        self.portfolio.build_model()
        self.logger.info(f'built model in {np.round(time.time() - start_time,2)} seconds')
        start_time = time.time()
        # Step 2: standard optimization --> returns power series in [kW]
        power_da = self.portfolio.optimize()
        self.logger.info(f'Finished day ahead optimization in {np.round(time.time() - start_time,2)} seconds')
        # Step 3: save optimization results in influxDB

        # df = pd.DataFrame(data=dict(powerTotal=power_da/10**3, heatTotal=self.portfolio.demand['heat']/10**3,
        #                             powerSolar=self.portfolio.generation['powerSolar']/10**3),
        #                   index=pd.date_range(start=self.date, freq='60min', periods=self.portfolio.T))
        #
        # self.connections['influxDB'].save_data(df, 'Areas', dict(typ=self.typ, agent=self.name, area=self.plz,
        #                                                          timestamp='optimize_dayAhead'))
        #
        # self.performance['saveSchedule'] = np.round(time.time() - start_time, 3)
        #
        # # Step 4: build orders from optimization results
        # # -------------------------------------------------------------------------------------------------------------
        # start_time = time.time()
        #
        # bid_orders = dict()
        # for i in range(self.portfolio.T):
        #     bid_orders.update({str(('dem%s' % i, i, 0, str(self.name))): (3000, power_da[i]/10**3, 'x')})
        #
        # self.performance['buildOrders'] = np.round(time.time() - start_time, 3)
        #
        # # Step 5: send orders to market resp. to mongodb
        # # -------------------------------------------------------------------------------------------------------------
        # start_time = time.time()
        #
        # self.connections['mongoDB'].set_dayAhead_orders(name=self.name, date=self.date, orders=bid_orders)
        #
        # self.performance['sendOrders'] = np.round(time.time() - start_time, 3)
        #
        # self.logger.info('DayAhead market scheduling completed')
        # print('DayAhead market scheduling completed:', self.name)

    def post_dayAhead(self):
        """Scheduling after DayAhead Market"""
        self.logger.info('After DayAhead market scheduling started')

        # Step 6: get market results and adjust generation an strategy
        # -------------------------------------------------------------------------------------------------------------
        start_time = time.time()

        # query the DayAhead results
        ask = self.connections['influxDB'].get_ask_da(self.date, self.name)            # volume to buy
        bid = self.connections['influxDB'].get_bid_da(self.date, self.name)            # volume to sell
        prc = self.connections['influxDB'].get_prc_da(self.date)                       # market clearing price
        profit = (ask - bid) * prc

        self.week_price_list.remember_price(prcToday=prc)

        power_da = np.asarray(self.portfolio.optimize(), np.float)                     # [kW]

        self.performance['adjustResult'] = time.time() - start_time

        # Step 7: save adjusted results in influxdb
        # -------------------------------------------------------------------------------------------------------------
        start_time = time.time()

        df = pd.DataFrame(data=dict(powerTotal=power_da/10**3, heatTotal=self.portfolio.demand['heat']/10**3,
                                    powerSolar=self.portfolio.generation['powerSolar']/10**3,
                                    profit=profit))
        df.index = pd.date_range(start=self.date, freq='60min', periods=len(df))
        self.connections['influxDB'].save_data(df, 'Areas', dict(typ=self.typ, agent=self.name, area=self.plz,
                                                                 timestamp='post_dayAhead'))

        self.performance['saveResult'] = np.round(time.time() - start_time, 3)

        self.logger.info('After DayAhead market adjustment completed')
        print('After DayAhead market adjustment completed:', self.name)
        self.logger.info('Next day scheduling started')

        # Step 8: retrain forecast methods and learning algorithm
        # -------------------------------------------------------------------------------------------------------------
        start_time = time.time()

        # No Price Forecast  used actually
        self.week_price_list.put_price()

        self.performance['nextDay'] = np.round(time.time() - start_time, 3)

        df = pd.DataFrame(data=self.performance, index=[self.date])
        self.connections['influxDB'].save_data(df, 'Performance', dict(typ=self.typ, agent=self.name, area=self.plz))

        self.logger.info('Next day scheduling completed')