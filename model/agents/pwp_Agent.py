# third party modules
import time
import pandas as pd
import numpy as np
import copy
from tqdm import tqdm

# model modules
from aggregation.portfolio_powerPlant import PwpPort
from agents.basic_Agent import BasicAgent
from forecasts.price import PriceForecast

class PwpAgent(BasicAgent):

    def __init__(self, date, plz, agent_type, connect,  infrastructure_source, infrastructure_login, *args, **kwargs):
        super().__init__(date, plz, agent_type, connect, infrastructure_source, infrastructure_login)
        self.logger.info('starting the agent')
        start_time = time.time()
        self.model_initiator = None
        self.step_width = [-10, -5, 0, 5, 10, 500]

        self.portfolio_1d = PwpPort(T=24, steps=self.step_width)
        self.portfolio_2d = PwpPort(T=48, steps=self.step_width)

        self.price_forecast = PriceForecast()

        for fuel in tqdm(['lignite', 'coal', 'gas', 'nuclear']):
            power_plants = self.infrastructure_interface.get_power_plant_in_area(area=plz, fuel_typ=fuel)
            if power_plants is not None:
                for system in power_plants.to_dict(orient='records'):
                    self.portfolio_1d.add_energy_system(system)
                    self.portfolio_2d.add_energy_system(system)

        # Construction power plants
        self.logger.info('Power Plants added')

        df = pd.DataFrame(index=[pd.to_datetime(self.date)], data=self.portfolio_1d.capacities)
        df['agent'] = self.name
        df.to_sql(name='installed capacities', con=self.simulation_database, if_exists='append')

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
        self.logger.info('DayAhead market scheduling started')
        start_time = time.time()

        prices = self.price_forecast.forecast(self.date)
        for key in ['power', 'gas', 'co']:
            prices[key] = np.repeat(prices[key], 2)

        self.model_initiator = copy.deepcopy({model.name:model.power_plant for model in self.portfolio_1d.energy_systems})
        def set_result(portfolio, offset, delta_t):
            for model in portfolio.energy_systems:
                for key in ['power', 'emission', 'fuel', 'start', 'obj']:
                    if key != 'obj':
                        model.data_storage[offset][key][delta_t:delta_t+portfolio.T] = model.power
                    else:
                        model.data_storage[offset][key] += np.round(np.sum(portfolio.prices['power'] * model.power), 2)
            return portfolio

        for offset in tqdm(self.step_width):
            for model in self.portfolio_1d.energy_systems:
                model.power_plant = self.model_initiator[model.name]
                model.data_storage[offset]['obj'] = 0
            for model in self.portfolio_2d.energy_systems:
                model.data_storage[offset]['obj'] = 0

            # prices and weather first day
            pr1 = dict(power=prices['power'][:24] + offset, gas=prices['gas'][:24], co=prices['co'][:24],
                       lignite=prices['lignite'], coal=prices['coal'], nuc=prices['nuc'])
            # prices and weather second day
            pr2 = dict(power=prices['power'][24:] + offset, gas=prices['gas'][24:], co=prices['co'][24:],
                       lignite=prices['lignite'], coal=prices['coal'], nuc=prices['nuc'])
            # prices and weather both days
            pr12 = dict(power=prices['power'] + offset, gas=prices['gas'], co=prices['co'],
                        lignite=prices['lignite'], coal=prices['coal'], nuc=prices['nuc'])

            # start first day
            self.portfolio_1d.set_parameter(date=self.date, weather=pd.DataFrame(), prices=pr1)
            self.portfolio_1d.build_model()
            power = self.portfolio_1d.optimize()
            self.portfolio_1d = set_result(portfolio=self.portfolio_1d, offset=offset, delta_t=0)
            self.portfolio_1d.build_model(response=power)
            self.portfolio_1d.optimize()

            # start second day
            self.portfolio_1d.set_parameter(date=self.date, weather=dict(), prices=pr2)
            self.portfolio_1d.build_model()
            self.portfolio_1d.optimize()
            self.portfolio_1d = set_result(portfolio=self.portfolio_1d, offset=offset, delta_t=24)

            # start shadow portfolio
            self.portfolio_2d.set_parameter(date=self.date, weather=dict(), prices=pr12)
            self.portfolio_2d.build_model()
            self.portfolio_2d.optimize()
            self.portfolio_2d = set_result(portfolio=self.portfolio_2d, offset=offset, delta_t=0)

        for model in self.portfolio_1d.energy_systems:
            model.power_plant = self.model_initiator[model.name]

        self.logger.info(f'Finished day ahead optimization in {np.round(time.time() - start_time, 2)} seconds')

        # Step 4: build orders from optimization results
        # -------------------------------------------------------------------------------------------------------------
        # start_time = tme.time()
        # order_book = {}
        #
        # for key, _ in self.portfolio.energy_systems.items():
        #
        #     # check if a start is prevented
        #     starts = {}
        #     d_delta = 0
        #     for offset in self.step_width:
        #         # a start can only prevented if the last power of the current day is zero
        #         starts.update({offset: dict(prevented=False, hours=[] , delta=0)})
        #         if self.portfolio_results[key][offset]['power'][23] == 0:
        #             # if the last power is zero, than one or more hours can be zero
        #             hours = np.argwhere(self.portfolio_results[key][offset]['power'][:24] == 0).reshape((-1,))
        #             # for these hours the power of the shadow portfolio must be greater than zero
        #             # then a start is prevented
        #             prevent_start = all(self.shadow_results[key][offset]['power'][hours] > 0)
        #             obj_portfolio = self.portfolio_results[key][offset]['obj']
        #             obj_shadow = self.shadow_results[key][offset]['obj']
        #             delta = obj_shadow - obj_portfolio
        #             # to implement an offset an additional profit of 5% must be reached
        #             percentage = delta / obj_portfolio if obj_portfolio else 0
        #             if prevent_start and percentage > 0.05:
        #                 starts.update({offset: dict(prevented=True, hours = hours, delta = delta - d_delta)})
        #
        #     last_power = np.zeros(24)                                               # last known power
        #     block_number = 0                                                        # block number counter
        #     links = {i: 'x' for i in range(24)}                                     # current links between blocks
        #     name = str(self.name + '-' + key)
        #     prevent_orders = {}
        #
        #     # build orders for each offset
        #     for offset in self.step_width:
        #         # get optimization result for key (block) and offset
        #         result = self.portfolio_results[key][offset]
        #
        #         # build mother order if any power > 0 for the current day and the block_number is zero
        #         if any(result['power'][:24] > 0) and block_number == 0:
        #             # for hours with power > 0 calculate mean variable costs
        #             hours = np.argwhere(result['power'][:24] > 0).reshape((-1,))
        #             costs = result['fuel'][hours] + result['emission'][hours] + result['start'][hours]
        #             var_costs = np.round(np.mean(costs / result['power'][hours]), 2)
        #             # for each hour with power > 0 add order to order_book
        #             for hour in hours:
        #                 price = var_costs
        #                 power = np.round(result['power'][hour], 2)
        #                 order_book.update({str(('gen0', hour, 0, name)): (price, power, 0)})
        #                 links.update({hour: block_number})
        #
        #             block_number += 1                                       # increment block number
        #             last_power = result['power'][:24]                       # set last_power to current power
        #             continue                                                # do next offset
        #
        #         # check if a start is prevented
        #         if starts[offset]['prevented']:
        #             result = self.shadow_results[key][offset]               # get shadow portfolio results
        #             hours = starts[offset]['hours']                         # get hours in which the start is prevented
        #             # calculate the reduction coefficient for each hour
        #             factor = starts[offset]['delta'] / np.sum(result['power'][hours])
        #             # if no orders already set, that prevent a start add new orders
        #             if len(prevent_orders) == 0:
        #                 # for each hour with power > 0 add order to order_book
        #                 for hour in hours:
        #                     costs = result['fuel'][hour] + result['emission'][hour]
        #                     price = var_costs = np.round(np.mean(costs / result['power'][hour]), 2)
        #                     power = np.round(result['power'][hour], 2)
        #                     order_book.update({str(('gen%s' % block_number, hour, 0, name)): (price, power, links[hour])})
        #                     prevent_orders.update({('gen%s' % block_number, hour, 0, name): (price, power, links[hour])})
        #                     links.update({hour: block_number})
        #                     block_number += 1                               # increment block number
        #             else:
        #                 # for each hour with power > 0 add order to order_book
        #                 # todo: if prices are too negative update this part
        #                 for hour in hours:
        #                     for id_, order in prevent_orders.items():
        #                         if id_[1] == hour:
        #                             order_to_prevent = {id_: (np.round(order[0] - factor, 2),
        #                                                       np.round(result['power'][hour], 2),
        #                                                       order[2])}
        #
        #                             order_to_book = {str(id_): (np.round(order[0] - factor, 2),
        #                                                         np.round(result['power'][hour], 2),
        #                                                         order[2])}
        #
        #                             prevent_orders.update(order_to_prevent)
        #                             order_book.update(order_to_book)
        #
        #             last_power[hours] = result['power'][hours]
        #             result = self.portfolio_results[key][offset]
        #
        #         # add linked hour blocks
        #         # check if current power is higher then the last known power
        #         if any(result['power'][:24] - last_power > 0):
        #             delta = result['power'][:24] - last_power  # get deltas
        #             stack_vertical = np.argwhere(last_power > 0).reshape((-1,))  # and check if last_power > 0
        #             # for each power with last_power > 0
        #             for hour in stack_vertical:
        #                 # check if delta > 0
        #                 if delta[hour] > 0:
        #                     # calculate variable cost for the hour and set it as requested price
        #                     price = np.round((result['fuel'][hour] + result['emission'][hour]) / result['power'][hour], 2)
        #                     power = np.round(0.2 * delta[hour], 2)
        #                     # check if the last linked block for this hour is unknown
        #                     if links[hour] == 'x':
        #                         link = 0  # if unknown, link to mother order
        #                     else:
        #                         link = links[hour]  # else link to last block for this hour
        #                     # split volume in five orders and add them to order_book
        #                     for order in range(5):
        #                         order_book.update({str(('gen%s' % block_number, hour, order, name)): (price,
        #                                                                                               power,
        #                                                                                               link)})
        #                     links.update({hour: block_number})  # update last known block for hour
        #                     block_number += 1  # increment block number
        #
        #             left = stack_vertical[0]    # get first left hour from last_power   ->  __|-----|__
        #             right = stack_vertical[-1]  # get first right hour from last_power  __|-----|__ <--
        #
        #             # if the left hour differs from first hour of the current day
        #             if left > 0:
        #                 # build array for e.g. [0,1,2,3,4,5, ..., left-1]
        #                 stack_left = np.arange(start=left - 1, stop=-1, step=-1)
        #                 # check if the last linked block for the fist left hour is unknown
        #                 # (only first hour is connected to mother)
        #                 if links[stack_left[0]] == 'x':
        #                     link = 0  # if unknown, link to mother order
        #                 else:
        #                     link = links[stack_left[0]]  # else link to last block for this hour
        #                 # for each hour in left_stack
        #                 for hour in stack_left:
        #                     # check if delta > 0
        #                     if delta[hour] > 0:
        #                         # calculate variable cost for the hour and set it as requested price
        #                         price = np.round((result['fuel'][hour] + result['emission'][hour]) / result['power'][hour], 2)
        #                         power = np.round(0.2 * delta[hour], 2)
        #                         # split volume in five orders and add them to order_book
        #                         for order in range(5):
        #                             order_book.update({str(('gen%s' % block_number, hour, order, name)): (price,
        #                                                                                                   power,
        #                                                                                                   link)})
        #                         link = block_number
        #                         links.update({hour: block_number})  # update last known block for hour
        #                         block_number += 1  # increment block number
        #
        #             # if the right hour differs from last hour of the current day
        #             if right < 23:
        #                 # build array for e.g. [right + 1, ... ,19,20,21,22,23]
        #                 stack_right = np.arange(start=right + 1, stop=24)
        #                 # check if the last linked block for the fist right hour is unknown
        #                 # (only first hour is connected to mother)
        #                 if links[stack_right[0]] == 'x':
        #                     link = 0  # if unknown, link to mother order
        #                 else:
        #                     link = links[stack_right[0]]  # else link to last block for this hour
        #                 for hour in stack_right:
        #                     # check if delta > 0
        #                     if delta[hour] > 0:
        #                         # calculate variable cost for the hour and set it as requested price
        #                         price = np.round((result['fuel'][hour] + result['emission'][hour]) / result['power'][hour], 2)
        #                         power = np.round(0.2 * delta[hour], 2)
        #                         # split volume in five orders and add them to order_boo
        #                         for order in range(5):
        #                             order_book.update({str(('gen%s' % block_number, hour, order, name)): (price,
        #                                                                                                   power,
        #                                                                                                   link)})
        #                         link = block_number
        #                         links.update({hour: block_number})  # update last known block for hour
        #                         block_number += 1  # increment block number
        #
        #             last_power = result['power'][:24]  # set last_power to current power
        #
        # self.performance['buildOrders'] = self.performance['initModel'] = np.round(tme.time() - start_time, 3)
        #
        # # Step 5: send orders to market resp. to mongodb
        # # -------------------------------------------------------------------------------------------------------------
        # start_time = tme.time()
        #
        # self.connections['mongoDB'].set_dayAhead_orders(name=self.name, date=self.date, orders=order_book)
        #
        # self.performance['sendOrders'] = tme.time() - start_time
        #
        # self.logger.info('DayAhead market scheduling completed')
        # print('DayAhead market scheduling completed:', self.name)

    def post_dayAhead(self):
        """Scheduling after DayAhead Market"""
        self.logger.info('After DayAhead market scheduling started')

        # Step 6: get market results and adjust generation
        # -------------------------------------------------------------------------------------------------------------
        start_time = time.time()

        # query the DayAhead results
        ask = self.connections['influxDB'].get_ask_da(self.date, self.name)            # volume to buy
        bid = self.connections['influxDB'].get_bid_da(self.date, self.name)            # volume to sell
        prc = self.connections['influxDB'].get_prc_da(self.date)                       # market clearing price
        profit = (ask - bid) * prc

        self.week_price_list.remember_price(prcToday=prc)

        # adjust power generation
        for key, value in self.portfolio_1d.energy_systems.items():
            value['model'].power_plant = copy.deepcopy(self.init_state[key])
        self.portfolio_1d.prices['power'][:len(prc)] = prc
        self.portfolio_1d.build_model(response=ask - bid)
        power_da, emission, fuel, _ = self.portfolio_1d.optimize()
        self.performance['adjustResult'] = self.performance['initModel'] = np.round(tme.time() - start_time, 3)

        # Step 7: save adjusted results in influxdb
        # -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()

        df = pd.concat([pd.DataFrame.from_dict(self.portfolio_1d.generation),
                        pd.DataFrame(data=dict(profit=profit, emissionAdjust=emission, fuelAdjust=fuel))], axis=1)
        df.index = pd.date_range(start=self.date, freq='60min', periods=len(df))
        self.connections['influxDB'].save_data(df, 'Areas', dict(typ=self.typ, agent=self.name, area=self.plz,
                                                                 timestamp='post_dayAhead'))

        self.performance['saveResult'] = self.performance['initModel'] = np.round(tme.time() - start_time, 3)

        self.logger.info('After DayAhead market adjustment completed')
        print('After DayAhead market adjustment completed:', self.name)
        self.logger.info('Next day scheduling started')

        self.__reset_results()

        # Step 8: retrain forecast methods and learning algorithm
        # -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()

        # collect data an retrain forecast method
        dem = self.connections['influxDB'].get_dem(self.date)                               # demand germany [MW]
        weather = self.forecasts['weather'].mean_weather                                    # weather data
        prc_1 = self.week_price_list.get_price_yesterday()                                  # mcp yesterday [€/MWh]
        prc_7 = self.week_price_list.get_price_week_before()                                # mcp week before [€/MWh]
        for key, method in self.forecasts.items():
            method.collect_data(date=self.date, dem=dem, prc=prc[:24], prc_1=prc_1, prc_7=prc_7, weather=weather)
            method.counter += 1
            if method.counter >= method.collect:  # retrain forecast method
                method.fit_function()
                method.counter = 0
                if key == 'price':
                    print(self.name, method.score)

        self.week_price_list.put_price()

        df = pd.DataFrame(index=[pd.to_datetime(self.date)], data=self.portfolio_1d.capacities)
        self.connections['influxDB'].save_data(df, 'Areas', dict(typ=self.typ, agent=self.name, area=self.plz))

        self.performance['nextDay'] = self.performance['initModel'] = np.round(tme.time() - start_time, 3)

        df = pd.DataFrame(data=self.performance, index=[self.date])
        self.connections['influxDB'].save_data(df, 'Performance', dict(typ=self.typ, agent=self.name, area=self.plz))

        self.logger.info('Next day scheduling completed')