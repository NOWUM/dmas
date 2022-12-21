import pandas as pd
from demandlib.electric_profile import get_holidays
from interfaces.weather import WeatherInterface
from skforecast.ForecasterAutoreg import ForecasterAutoreg
from sklearn.neural_network import MLPRegressor
from sklearn.pipeline import make_pipeline
# from sklearn.linear_model import Ridge
from sklearn.preprocessing import MinMaxScaler

DATABASE_URI = 'postgresql://opendata:opendata@10.13.10.41:5432/weather'

weather = WeatherInterface(database_url=DATABASE_URI, name='testing')
weather_params = ['temp_air', 'wind_meridional', 'wind_zonal', 'dhi', 'dni']

def get_dummies(price_data: pd.DataFrame):
    hours = pd.get_dummies(price_data.index.hour)
    hours.columns = [f'hour_{h}' for h in hours.columns]
    week_days = pd.get_dummies(price_data.index.day_name())
    month_of_year = pd.get_dummies(price_data.index.month_name())
    week_of_year = pd.get_dummies(price_data.index.weekofyear)
    week_of_year.columns = [f'week_{c}' for c in week_of_year.columns]
    years = price_data.index.year.unique()
    dummies = pd.concat([hours, week_days, month_of_year, week_of_year], axis=1)
    dummies['holiday'] = 0
    dummies.index = price_data.index
    for year in years:
        holidays = get_holidays(year=year)
        for day in holidays:
            dummies.loc[(dummies.index.dayofyear == day) & (dummies.index.year == year), 'holiday'] = 1
    return dummies


def get_weather_data(date_range: pd.DatetimeIndex):
    print('collecting weather...')
    data = {param: [] for param in weather_params}

    for date in date_range:
        for param in weather_params:
            df = weather.get_param(param, date)
            values = list(df.values.flatten())
            data[param] += values
    data = pd.DataFrame.from_dict(data)
    data['wind_speed'] = (data['wind_meridional'].values**2 + data['wind_zonal'].values**2)**0.5
    data['ghi'] = data['dni'] + data['dhi']
    del data['wind_meridional']
    del data['wind_zonal']
    return data


if __name__ == "__main__":
    hourly_range = pd.date_range(start='2017-01-01', end='2018-12-31 23:00', freq='h')
    days = pd.date_range(start='2017-01-01', end='2018-12-31', freq='d')
    weather_data = get_weather_data(days)
    weather_data.index = prices.index
    dummies = get_dummies(prices)
    exog = pd.concat([weather_data, dummies], axis=1)
    end_validation = pd.Timestamp(2018, 9, 30)

    print('fitting model...')
    lags_grid = [[1, 2, 24], [1, 2, 3, 4, 5, 24], [1, 2, 3, 4, 24, 48], [1, 24], [1, 2, 3, 23, 24, 25, 47, 48]]
    param_grid = {'hidden_layer_sizes': [(15, 15, ), (30, 30, ), (100, 100,)], 'learning_rate_init': [0.02, 0.05, 0.10]}
    model = MLPRegressor(hidden_layer_sizes=(25, 25,), activation='identity', early_stopping=True,
                         solver='adam', learning_rate_init=0.02, shuffle=True, max_iter=500)
    forecaster = ForecasterAutoreg(regressor=make_pipeline(MinMaxScaler(), model), lags=[1, 2, 23, 24, 25, 48])

    # results_grid = grid_search_forecaster(
    #     forecaster=forecaster,
    #     y=prices.loc[:end_validation, 'price'],
    #     param_grid=param_grid,
    #     lags_grid=lags_grid,
    #     steps=24,
    #     exog=exog.loc[:end_validation, :],
    #     metric='mean_absolute_error',
    #     refit=False,
    #     initial_train_size=len(prices.loc[:pd.Timestamp(2018, 6, 1), 'price']),
    #     fixed_train_size=False,
    #     return_best=True,
    #     verbose=False
    # )
    # forecaster = ForecasterAutoreg(regressor=make_pipeline(StandardScaler(), Ridge(alpha=27.8)), lags=[1, 24])
    # forecaster.fit(y=prices.loc[:end_validation, 'price'], exog=exog.loc[:end_validation, :])
    # test_range = days[days > end_validation]
    #
    # predictions = []
    # for day in test_range:
    #     next_day = day + td(days=1)
    #     offset = day - td(days=2)
    #     prediction = forecaster.predict(steps=24, last_window=prices.loc[offset:day - td(hours=1), 'price'],
    #                                     exog=exog.loc[day:next_day-td(hours=1)])
    #     predictions += list(prediction.values)
    #
    # true_values = prices.loc[prices.index > end_validation + td(hours=23)].values.flatten()
    # print(mean_squared_error(true_values, predictions))
    # plt.plot(predictions)
    # plt.plot(true_values)
    #smoothed = prices.ewm(com=24).mean()
    #plt.plot(prices.values)
    #plt.plot(smoothed.values)
    #plt.show()
    # prices.plot()
    # smoothed.plot()