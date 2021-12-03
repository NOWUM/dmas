# third party modules
import time as tme
import pandas as pd
import numpy as np
import copy


# model modules
from aggregation.portfolio_powerPlant import PwpPort
from agents.client_Agent import agent as basicAgent


class PwpAgent(basicAgent):

    def __init__(self, date, plz, agent_type, mqtt_exchange, connect,  infrastructure_source, infrastructure_login):
        super().__init__(date, plz, agent_type, mqtt_exchange, connect, infrastructure_source, infrastructure_login)
        # Development of the portfolio with the corresponding power plants and storages

        self.logger.info('starting the agent')
        start_time = tme.time()
        fuel_typs = ['lignite', 'coal', 'gas', 'nuclear']
        self.init_state = {}
        self.step_width = [-10, -5, 0, 5, 10, 500]

        self.portfolio = PwpPort(T=24)
        self.x = self.infrastructure_interface.get_power_plant_in_area(area=plz, fuel_typ='lignite')
        self.shadow_portfolio = PwpPort(T=48)

        for fuel in ['lignite', 'coal', 'gas', 'nuclear']:
            power_plants = self.infrastructure_interface.get_power_plant_in_area(area=plz, fuel_typ=fuel)
            if power_plants is not None:
                for _, data in power_plants.iterrows():
                    self.portfolio.add_energy_system(data.to_dict())

        # Construction power plants
        self.logger.info('Power Plants added')

        df = pd.DataFrame(index=[pd.to_datetime(self.date)], data=self.portfolio.capacities)
        df.to_sql(name=self.name, con=self.simulation_database)
        # self.connections['influxDB'].save_data(df, 'Areas', dict(typ=self.typ, agent=self.name, area=self.plz))

        # initialize dicts for optimization results
        self.portfolio_results = {key: {offset: dict(power=np.array([]),
                                                     emission=np.array([]),
                                                     fuel=np.array([]),
                                                     start=np.array([]),
                                                     obj=0)
                                        for offset in self.step_width }
                                  for key, _ in self.portfolio.energy_systems.items()}

        self.shadow_results = {key: {offset: dict(power=np.array([]),
                                                  emission=np.array([]),
                                                  fuel=np.array([]),
                                                  start=np.array([]),
                                                  obj=0)
                                     for offset in self.step_width }
                               for key, _ in self.portfolio.energy_systems.items()}

        self.logger.info('setup of the agent completed in %s' % (tme.time() - start_time))

    @staticmethod
    def __set_results(portfolio, result, offset, price):
        for key, value in portfolio.energy_systems.items():
            result[key][offset]['power'] = np.concatenate((result[key][offset]['power'], value['model'].power))
            result[key][offset]['emission'] = np.concatenate((result[key][offset]['emission'], value['model'].emission))
            result[key][offset]['fuel'] = np.concatenate((result[key][offset]['fuel'], value['model'].fuel))
            result[key][offset]['start'] = np.concatenate((result[key][offset]['start'], value['model'].start))
            obj = np.sum(value['model'].power * price['power']) \
                  - np.sum(value['model'].emission) - np.sum(value['model'].fuel) - np.sum(value['model'].start)
            result[key][offset]['obj'] += obj

    def __reset_results(self):
        self.portfolio_results = {key: {offset: dict(power=np.array([]),
                                                     emission=np.array([]),
                                                     fuel=np.array([]),
                                                     start=np.array([]),
                                                     obj=0)
                                        for offset in self.step_width }
                                  for key, _ in self.portfolio.energy_systems.items()}

        self.shadow_results = {key: {offset: dict(power=np.array([]),
                                                  emission=np.array([]),
                                                  fuel=np.array([]),
                                                  start=np.array([]),
                                                  obj=0)
                                     for offset in self.step_width }
                               for key, _ in self.portfolio.energy_systems.items()}

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
        start_time = tme.time()

        prices = self.price_forecast(self.date, days=2)                        # price forecast dayAhead
        self.performance['initModel'] = self.performance['initModel'] = np.round(tme.time() - start_time, 3)

        init_state = {key: value['model'].power_plant for key, value in self.portfolio.energy_systems.items()}
        #return init_state


        self.init_state = copy.deepcopy(init_state)

        # Step 2: optimization --> returns power series in [MW]
        # -------------------------------------------------------------------------------------------------------------
        for offset in self.step_width :
            for key, value in self.portfolio.energy_systems.items():
                value['model'].power_plant = copy.deepcopy(self.init_state[key])
            for key, value in self.shadow_portfolio.energy_systems.items():
                value['model'].power_plant = copy.deepcopy(self.init_state[key])

            # prices and weather first day
            pr1 = dict(power=prices['power'][:24] + offset, gas=prices['gas'][:24], co=prices['co'][:24],
                       lignite=prices['lignite'], coal=prices['coal'], nuc=prices['nuc'])
            # prices and weather second day
            pr2 = dict(power=prices['power'][24:] + offset, gas=prices['gas'][24:], co=prices['co'][24:],
                       lignite=prices['lignite'], coal=prices['coal'], nuc=prices['nuc'])
            # prices and weather both days
            pr12 = dict(power=prices['power'] + offset, gas=prices['gas'], co=prices['co'],
                        lignite=prices['lignite'], coal=prices['coal'], nuc=prices['nuc'])

            self.portfolio.set_parameter(date=self.date, weather=dict(), prices=pr1)
            self.portfolio.build_model()
            power, _, _, _ = self.portfolio.optimize()

            if offset == 0:
                df = pd.DataFrame.from_dict(self.portfolio.generation)

            self.__set_results(portfolio=self.portfolio, offset=offset, result=self.portfolio_results,
                               price=pr1)

            self.portfolio.build_model(response=power)
            self.portfolio.optimize()

            self.portfolio.set_parameter(date=self.date + pd.DateOffset(days=1), weather=dict(), prices=pr2)
            self.portfolio.build_model()
            self.portfolio.optimize()

            self.__set_results(portfolio=self.portfolio, offset=offset, result=self.portfolio_results,
                               price=pr2)

            self.shadow_portfolio.set_parameter(date=self.date, weather=dict(), prices=pr12)
            self.shadow_portfolio.build_model()
            self.shadow_portfolio.optimize()

            self.__set_results(portfolio=self.shadow_portfolio, offset=offset, result=self.shadow_results,
                               price=pr12)

        self.performance['optModel'] = tme.time() - start_time

        # Step 3: save optimization results in influxDB
        # -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()

        # build dataframe to save results in ifluxdb
        df = pd.concat([df, pd.DataFrame(data=dict(frcst=prices['power'][:24]))], axis=1)
        df.index = pd.date_range(start=self.date, freq='60min', periods=len(df))
        self.connections['influxDB'].save_data(df, 'Areas', dict(typ=self.typ, agent=self.name, area=self.plz,
                                                                 timestamp='optimize_dayAhead'))

        self.performance['saveSchedule'] = tme.time() - start_time

        # Step 4: build orders from optimization results
        # -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()
        order_book = {}

        for key, _ in self.portfolio.energy_systems.items():

            # check if a start is prevented
            starts = {}
            d_delta = 0
            for offset in self.step_width:
                # a start can only prevented if the last power of the current day is zero
                starts.update({offset: dict(prevented=False, hours=[] , delta=0)})
                if self.portfolio_results[key][offset]['power'][23] == 0:
                    # if the last power is zero, than one or more hours can be zero
                    hours = np.argwhere(self.portfolio_results[key][offset]['power'][:24] == 0).reshape((-1,))
                    # for these hours the power of the shadow portfolio must be greater than zero
                    # then a start is prevented
                    prevent_start = all(self.shadow_results[key][offset]['power'][hours] > 0)
                    obj_portfolio = self.portfolio_results[key][offset]['obj']
                    obj_shadow = self.shadow_results[key][offset]['obj']
                    delta = obj_shadow - obj_portfolio
                    # to implement an offset an additional profit of 5% must be reached
                    percentage = delta / obj_portfolio if obj_portfolio else 0
                    if prevent_start and percentage > 0.05:
                        starts.update({offset: dict(prevented=True, hours = hours, delta = delta - d_delta)})

            last_power = np.zeros(24)                                               # last known power
            block_number = 0                                                        # block number counter
            links = {i: 'x' for i in range(24)}                                     # current links between blocks
            name = str(self.name + '-' + key)
            prevent_orders = {}

            # build orders for each offset
            for offset in self.step_width:
                # get optimization result for key (block) and offset
                result = self.portfolio_results[key][offset]

                # build mother order if any power > 0 for the current day and the block_number is zero
                if any(result['power'][:24] > 0) and block_number == 0:
                    # for hours with power > 0 calculate mean variable costs
                    hours = np.argwhere(result['power'][:24] > 0).reshape((-1,))
                    costs = result['fuel'][hours] + result['emission'][hours] + result['start'][hours]
                    var_costs = np.round(np.mean(costs / result['power'][hours]), 2)
                    # for each hour with power > 0 add order to order_book
                    for hour in hours:
                        price = var_costs
                        power = np.round(result['power'][hour], 2)
                        order_book.update({str(('gen0', hour, 0, name)): (price, power, 0)})
                        links.update({hour: block_number})

                    block_number += 1                                       # increment block number
                    last_power = result['power'][:24]                       # set last_power to current power
                    continue                                                # do next offset

                # check if a start is prevented
                if starts[offset]['prevented']:
                    result = self.shadow_results[key][offset]               # get shadow portfolio results
                    hours = starts[offset]['hours']                         # get hours in which the start is prevented
                    # calculate the reduction coefficient for each hour
                    factor = starts[offset]['delta'] / np.sum(result['power'][hours])
                    # if no orders already set, that prevent a start add new orders
                    if len(prevent_orders) == 0:
                        # for each hour with power > 0 add order to order_book
                        for hour in hours:
                            costs = result['fuel'][hour] + result['emission'][hour]
                            price = var_costs = np.round(np.mean(costs / result['power'][hour]), 2)
                            power = np.round(result['power'][hour], 2)
                            order_book.update({str(('gen%s' % block_number, hour, 0, name)): (price, power, links[hour])})
                            prevent_orders.update({('gen%s' % block_number, hour, 0, name): (price, power, links[hour])})
                            links.update({hour: block_number})
                            block_number += 1                               # increment block number
                    else:
                        # for each hour with power > 0 add order to order_book
                        # todo: if prices are too negative update this part
                        for hour in hours:
                            for id_, order in prevent_orders.items():
                                if id_[1] == hour:
                                    order_to_prevent = {id_: (np.round(order[0] - factor, 2),
                                                              np.round(result['power'][hour], 2),
                                                              order[2])}

                                    order_to_book = {str(id_): (np.round(order[0] - factor, 2),
                                                                np.round(result['power'][hour], 2),
                                                                order[2])}

                                    prevent_orders.update(order_to_prevent)
                                    order_book.update(order_to_book)

                    last_power[hours] = result['power'][hours]
                    result = self.portfolio_results[key][offset]

                # add linked hour blocks
                # check if current power is higher then the last known power
                if any(result['power'][:24] - last_power > 0):
                    delta = result['power'][:24] - last_power  # get deltas
                    stack_vertical = np.argwhere(last_power > 0).reshape((-1,))  # and check if last_power > 0
                    # for each power with last_power > 0
                    for hour in stack_vertical:
                        # check if delta > 0
                        if delta[hour] > 0:
                            # calculate variable cost for the hour and set it as requested price
                            price = np.round((result['fuel'][hour] + result['emission'][hour]) / result['power'][hour], 2)
                            power = np.round(0.2 * delta[hour], 2)
                            # check if the last linked block for this hour is unknown
                            if links[hour] == 'x':
                                link = 0  # if unknown, link to mother order
                            else:
                                link = links[hour]  # else link to last block for this hour
                            # split volume in five orders and add them to order_book
                            for order in range(5):
                                order_book.update({str(('gen%s' % block_number, hour, order, name)): (price,
                                                                                                      power,
                                                                                                      link)})
                            links.update({hour: block_number})  # update last known block for hour
                            block_number += 1  # increment block number

                    left = stack_vertical[0]    # get first left hour from last_power   ->  __|-----|__
                    right = stack_vertical[-1]  # get first right hour from last_power  __|-----|__ <--

                    # if the left hour differs from first hour of the current day
                    if left > 0:
                        # build array for e.g. [0,1,2,3,4,5, ..., left-1]
                        stack_left = np.arange(start=left - 1, stop=-1, step=-1)
                        # check if the last linked block for the fist left hour is unknown
                        # (only first hour is connected to mother)
                        if links[stack_left[0]] == 'x':
                            link = 0  # if unknown, link to mother order
                        else:
                            link = links[stack_left[0]]  # else link to last block for this hour
                        # for each hour in left_stack
                        for hour in stack_left:
                            # check if delta > 0
                            if delta[hour] > 0:
                                # calculate variable cost for the hour and set it as requested price
                                price = np.round((result['fuel'][hour] + result['emission'][hour]) / result['power'][hour], 2)
                                power = np.round(0.2 * delta[hour], 2)
                                # split volume in five orders and add them to order_book
                                for order in range(5):
                                    order_book.update({str(('gen%s' % block_number, hour, order, name)): (price,
                                                                                                          power,
                                                                                                          link)})
                                link = block_number
                                links.update({hour: block_number})  # update last known block for hour
                                block_number += 1  # increment block number

                    # if the right hour differs from last hour of the current day
                    if right < 23:
                        # build array for e.g. [right + 1, ... ,19,20,21,22,23]
                        stack_right = np.arange(start=right + 1, stop=24)
                        # check if the last linked block for the fist right hour is unknown
                        # (only first hour is connected to mother)
                        if links[stack_right[0]] == 'x':
                            link = 0  # if unknown, link to mother order
                        else:
                            link = links[stack_right[0]]  # else link to last block for this hour
                        for hour in stack_right:
                            # check if delta > 0
                            if delta[hour] > 0:
                                # calculate variable cost for the hour and set it as requested price
                                price = np.round((result['fuel'][hour] + result['emission'][hour]) / result['power'][hour], 2)
                                power = np.round(0.2 * delta[hour], 2)
                                # split volume in five orders and add them to order_boo
                                for order in range(5):
                                    order_book.update({str(('gen%s' % block_number, hour, order, name)): (price,
                                                                                                          power,
                                                                                                          link)})
                                link = block_number
                                links.update({hour: block_number})  # update last known block for hour
                                block_number += 1  # increment block number

                    last_power = result['power'][:24]  # set last_power to current power

        self.performance['buildOrders'] = self.performance['initModel'] = np.round(tme.time() - start_time, 3)

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

        self.week_price_list.remember_price(prcToday=prc)

        # adjust power generation
        for key, value in self.portfolio.energy_systems.items():
            value['model'].power_plant = copy.deepcopy(self.init_state[key])
        self.portfolio.prices['power'][:len(prc)] = prc
        self.portfolio.build_model(response=ask - bid)
        power_da, emission, fuel, _ = self.portfolio.optimize()
        self.performance['adjustResult'] = self.performance['initModel'] = np.round(tme.time() - start_time, 3)

        # Step 7: save adjusted results in influxdb
        # -------------------------------------------------------------------------------------------------------------
        start_time = tme.time()

        df = pd.concat([pd.DataFrame.from_dict(self.portfolio.generation),
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

        df = pd.DataFrame(index=[pd.to_datetime(self.date)], data=self.portfolio.capacities)
        self.connections['influxDB'].save_data(df, 'Areas', dict(typ=self.typ, agent=self.name, area=self.plz))

        self.performance['nextDay'] = self.performance['initModel'] = np.round(tme.time() - start_time, 3)

        df = pd.DataFrame(data=self.performance, index=[self.date])
        self.connections['influxDB'].save_data(df, 'Performance', dict(typ=self.typ, agent=self.name, area=self.plz))

        self.logger.info('Next day scheduling completed')