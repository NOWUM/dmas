# third party modules
import time
import pandas as pd
import numpy as np
from tqdm import tqdm

# model modules
from forecasts.price import PriceForecast
from forecasts.weather import WeatherForecast
from aggregation.portfolio_powerPlant import PowerPlantPortfolio
from agents.basic_Agent import BasicAgent


class PwpAgent(BasicAgent):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        start_time = time.time()

        self.portfolio = PowerPlantPortfolio()

        self.weather_forecast = WeatherForecast(position=dict(lat=self.latitude, lon=self.longitude),
                                                simulation_interface=self.simulation_interface,
                                                weather_interface=self.weather_interface)
        self.price_forecast = PriceForecast(position=dict(lat=self.latitude, lon=self.longitude),
                                            simulation_interface=self.simulation_interface,
                                            weather_interface=self.weather_interface)
        self.forecast_counter = 10

        for fuel in tqdm(['lignite', 'coal', 'gas', 'nuclear']):
            power_plants = self.infrastructure_interface.get_power_plant_in_area(self.area, fuel_type=fuel)
            if not power_plants.empty:
                for system in power_plants.to_dict(orient='records'):
                    self.portfolio.add_energy_system(system)

        # Construction power plants
        self.logger.info('Power Plants added')

        self.logger.info(f'setup of the agent completed in {time.time() - start_time:.2f} seconds')


    def callback(self, ch, method, properties, body):
        super().callback(ch, method, properties, body)

        message = body.decode("utf-8")
        self.date = pd.to_datetime(message.split(' ')[1])
        self.logger.info(f'get command {message}')

        if 'set_capacities' in message:
            self.simulation_interface.set_capacities(self.portfolio,self.area, self.date)
        if 'opt_dayAhead' in message:
            self.optimize_day_ahead()
        if 'result_dayAhead' in message:
            self.post_day_ahead()

    def get_order_book(self):
        order_book = {}

        for model in self.portfolio.energy_systems:

            last_power = np.zeros(24)  # last known power
            block_number = 0  # block number counter
            links = {i: -1 for i in range(24)}  # current links between blocks
            name = model.name
            prevent_orders = {}

            # build orders for each offset
            for offset in self.portfolio.steps:
                # get optimization result for key (block) and offset
                result = model.optimization_results[offset]
                starts = model.prevented_start[offset]

                # build mother order if any power > 0 for the current day and the block_number is zero
                if any(result['power'] > 0) and block_number == 0:
                    # for hours with power > 0 calculate mean variable costs
                    hours = np.argwhere(result['power'] > 0).reshape((-1,))
                    costs = result['fuel'][hours] + result['emission'][hours] + result['start'][hours]
                    var_costs = np.round(np.mean(costs / result['power'][hours]), 2)
                    # for each hour with power > 0 add order to order_book
                    for hour in hours:
                        price = var_costs
                        power = np.round(result['power'][hour], 2)
                        order_book.update({(0, hour, 0, name): (price, power, 0)})
                        links.update({hour: block_number})

                    block_number += 1  # increment block number
                    last_power = result['power']  # set last_power to current power
                    continue  # do next offset

                # check if a start (and stop) is prevented
                if starts['prevent_start']:
                    hours = starts['hours']  # get hours in which the start is prevented
                    count = len(hours)
                    p_min = model.power_plant['PMin']
                    # calculate the reduction coefficient for each hour
                    factor = starts['delta'] / np.sum(p_min * count)
                    # if no orders that prevent a start are already set add new orders
                    if len(prevent_orders) == 0:
                        # for each hour with power > 0 add order to order_book
                        for hour in hours:
                            costs = model.prices['fuel'][hour] + model.prices['emission'][hour]
                            var_costs = np.round(costs / p_min, 2)
                            power = np.round(p_min, 2)
                            order_book.update({(block_number, hour, 0, name): (var_costs, power, links[hour])})
                            prevent_orders.update({(block_number, hour, 0, name): (var_costs, power, links[hour])})
                            links.update({hour: block_number})
                            block_number += 1  # increment block number
                    else:
                        # for each hour with power > 0 add order to order_book
                        # todo: if prices are too negative update this part
                        for hour in hours:
                            for id_, order in prevent_orders.items():
                                if id_[1] == hour:
                                    order_to_prevent = {id_: (np.round(order[0] - factor, 2),
                                                              np.round(result['power'][hour], 2),
                                                              order[2])}

                                    order_to_book = {id_: (np.round(order[0] - factor, 2),
                                                           np.round(result['power'][hour], 2),
                                                           order[2])}

                                    prevent_orders.update(order_to_prevent)
                                    order_book.update(order_to_book)

                    last_power[hours] = result['power'][hours]
                    result = model.optimization_results[offset]

                # add linked hour blocks
                # check if current power is higher then the last known power
                if any(result['power'] - last_power > 0):
                    delta = result['power'] - last_power  # get deltas
                    stack_vertical = np.argwhere(last_power > 0).reshape((-1,))  # and check if last_power > 0
                    # for each power with last_power > 0
                    for hour in stack_vertical:
                        # check if delta > 0
                        if delta[hour] > 0:
                            # calculate variable cost for the hour and set it as requested price
                            price = np.round(
                                (result['fuel'][hour] + result['emission'][hour]) / result['power'][hour], 2)
                            power = np.round(0.2 * delta[hour], 2)
                            # check if the last linked block for this hour is unknown
                            if links[hour] == -1:
                                link = 0  # if unknown, link to mother order
                            else:
                                link = links[hour]  # else link to last block for this hour
                            # split volume in five orders and add them to order_book
                            for order in range(5):
                                order_book.update({(block_number, hour, order, name): (price, power, link)})
                            links.update({hour: block_number})  # update last known block for hour
                            block_number += 1  # increment block number

                    left = stack_vertical[0]  # get first left hour from last_power   ->  __|-----|__
                    right = stack_vertical[-1]  # get first right hour from last_power  __|-----|__ <--

                    # if the left hour differs from first hour of the current day
                    if left > 0:
                        # build array for e.g. [0,1,2,3,4,5, ..., left-1]
                        stack_left = np.arange(start=left - 1, stop=-1, step=-1)
                        # check if the last linked block for the fist left hour is unknown
                        # (only first hour is connected to mother)
                        if links[stack_left[0]] == -1:
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
                                    order_book.update({(block_number, hour, order, name): (price, power, link)})
                                link = block_number
                                links.update({hour: block_number})  # update last known block for hour
                                block_number += 1  # increment block number

                    # if the right hour differs from last hour of the current day
                    if right < 23:
                        # build array for e.g. [right + 1, ... ,19,20,21,22,23]
                        stack_right = np.arange(start=right + 1, stop=24)
                        # check if the last linked block for the fist right hour is unknown
                        # (only first hour is connected to mother)
                        if links[stack_right[0]] == -1:
                            link = 0  # if unknown, link to mother order
                        else:
                            link = links[stack_right[0]]  # else link to last block for this hour
                        for hour in stack_right:
                            # check if delta > 0
                            if delta[hour] > 0:
                                # calculate variable cost for the hour and set it as requested price
                                price = np.round(
                                    (result['fuel'][hour] + result['emission'][hour]) / result['power'][hour], 2)
                                power = np.round(0.2 * delta[hour], 2)
                                # split volume in five orders and add them to order_book
                                for order in range(5):
                                    order_book.update({(block_number, hour, order, name): (price,
                                                                                           power,
                                                                                           link)})
                                link = block_number
                                links.update({hour: block_number})  # update last known block for hour
                                block_number += 1  # increment block number

                    last_power = result['power']  # set last_power to current power
        if order_book:
            df = pd.DataFrame.from_dict(order_book, orient='index')
        else:
            # if nothing in self.portfolio.energy_systems
            df = pd.DataFrame(columns=['price', 'volume', 'link', 'type'])
        
        df['type'] = 'generation'
        df.columns = ['price', 'volume', 'link', 'type']
        # as values are used from energy_systems, they are in €/kW
        # we convert them to €/MW to send them to the market
        df['price'] /= 1e3
        df.index = pd.MultiIndex.from_tuples(df.index, names=['block_id', 'hour', 'order_id', 'name'])

        return df

    def optimize_day_ahead(self):
        """scheduling for the DayAhead market"""
        self.logger.info('dayAhead market scheduling started')
        start_time = time.time()

        # Step 1: forecast data data and init the model for the coming day
        weather = self.weather_forecast.forecast_for_area(self.date, self.area)
        prices = self.price_forecast.forecast(self.date)
        prices = pd.concat([prices, prices.copy()])
        prices.index = pd.date_range(start=self.date, freq='h', periods=48)

        self.portfolio.set_parameter(self.date, weather.copy(), prices.copy())
        self.portfolio.build_model()
        self.logger.info(f'built model in {time.time() - start_time:.2f} seconds')
        # Step 2: optimization
        self.portfolio.optimize()
        self.logger.info(f'finished day ahead optimization in {time.time() - start_time:.2f} seconds')

        # save optimization results
        self.simulation_interface.set_generation(self.portfolio, 'optimize_dayAhead', self.area, self.date)
        self.simulation_interface.set_demand(self.portfolio, 'optimize_dayAhead', self.area, self.date)

        # Step 3: build orders from optimization results
        start_time = time.time()
        order_book = self.get_order_book()
        self.simulation_interface.set_linked_orders(order_book)
        self.publish.basic_publish(exchange=self.mqtt_exchange, routing_key='', body=f'{self.name} {self.date.date()}')

        self.logger.info(f'built Orders in {time.time() - start_time:.2f} seconds')

    def post_day_ahead(self):
        """Scheduling after DayAhead Market"""
        self.logger.info('starting day ahead adjustments')

        start_time = time.time()

        # query the DayAhead results
        for model in self.portfolio.energy_systems:
            power = np.zeros(24)
            committed_power = self.simulation_interface.get_linked_result(model.name)
            for index, row in committed_power.iterrows():
                power[int(row.hour)] = float(row.volume)

            model.committed_power = power
            model.build_model()

        self.portfolio.optimize()

        # save optimization results
        self.simulation_interface.set_generation(self.portfolio, 'post_dayAhead', self.area, self.date)
        self.simulation_interface.set_demand(self.portfolio, 'post_dayAhead', self.area, self.date)

        self.weather_forecast.collect_data(self.date)
        self.price_forecast.collect_data(self.date)
        self.forecast_counter -= 1

        if self.forecast_counter == 0:
            self.price_forecast.fit_model()
            self.forecast_counter = 10
            self.logger.info(f'fitted price forecast with R²: {self.price_forecast.score}')

        self.logger.info(f'finished day ahead adjustments in {time.time() - start_time:.2f} seconds')
