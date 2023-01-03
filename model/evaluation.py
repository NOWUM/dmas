'''
This file compares different simulation results in the same time frame
and calculates metrics in comparison to the ground truth of ENTSO-E.

The plots for the initial paper are also generated with these functions.
'''

import numpy as np
import pandas as pd
from interfaces.import_export import EntsoeInfrastructureInterface
from interfaces.simulation import SimulationInterface
from datetime import timedelta
from scipy import stats
ENTSOE_DB_URI = 'postgresql://readonly:readonly@10.13.10.41:5432/entsoe'

db_uris = {
    'nuts1': 'postgresql://dMAS:dMAS@10.13.10.54:6431/dmas',
    'nuts2': 'postgresql://dMAS:dMAS@10.13.10.54:6432/dmas',
    'nuts3': 'postgresql://dMAS:dMAS@10.13.10.54:6433/dmas',
#    'nuts3a': 'postgresql://dMAS:dMAS@10.13.10.54:6434/dmas',
    'db_uri': 'postgresql://dMAS:dMAS@10.13.10.54:5432/dmas',
}


def rmse(residuen):
    return np.sqrt(np.mean((residuen)**2))


def mae(residuen):
    return np.mean(np.abs(residuen))

# TODO test different intervals to check that assumptions are reliable

if __name__ == '__main__':
    entsoe = EntsoeInfrastructureInterface('validation', ENTSOE_DB_URI)
    results = pd.DataFrame()
    results_rmse = pd.DataFrame()
    correlation = pd.DataFrame()
    for name, db_uri in db_uris.items():
        sim_results = SimulationInterface('eval', db_uri)
        start, end = sim_results.get_sim_start_and_end()
        end += timedelta(days=1)
        print(name, start.date(), end.date())

        generation_truth = entsoe.get_generation_in_land('DE', start, end)
        price_truth = entsoe.get_price_in_land('DE', start, end)
        demand_truth = entsoe.get_demand_in_land('DE', start, end)
        generation_truth['price'] = price_truth['da_price']
        generation_truth['load'] = demand_truth['actual_load']

        generation = sim_results.get_generation(start, step='post_dayAhead')
        prices = sim_results.get_auction_results_range(start, end)
        generation['price'] = prices['price']
        generation['load'] = prices['volume']
        del generation_truth['other']
        del generation['total']

        fr = 500
        to = 800
        generation = generation[fr:to]
        generation_truth = generation_truth[fr:to]

        normalized_truth = (generation_truth - generation_truth.mean()) / generation_truth.std()
        normalized = (generation - generation.mean()) / generation.std()

        diff = generation_truth - generation
        diff_normalized = normalized_truth - normalized

        results[f'mae_{name}'] = diff.apply(mae)/1e3 # MWh
        results[f'norm_mae_{name}'] = diff_normalized.apply(mae) # MWh

        results_rmse[f'rmse_{name}'] = diff.apply(rmse)/1e3 # MWh
        results_rmse[f'norm_rmse_{name}'] = diff_normalized.apply(rmse)

        s = pd.Series(dtype=float)
        s_norm = pd.Series(dtype=float)
        for column in generation.columns:
            s[column] = stats.spearmanr(generation[column], generation_truth[column]).correlation
            s_norm[column] = stats.spearmanr(normalized[column], normalized_truth[column]).correlation
        correlation[f'corr_{name}'] = s
        # correlation[f'norm_corr_{name}'] = s_norm

    results['mae_mean'] = normalized_truth.apply(mae)
    results_rmse['rmse_mean'] = normalized_truth.apply(rmse)

    results.to_csv('mae_results.csv')
    results_rmse.to_csv('rmse_results.csv')
    correlation.to_csv('correlation_results.csv')

    import matplotlib.pyplot as plt
    fr = 0
    to = -1
    for power in ['solar', 'wind', 'coal']:
        plt.title(power)
        plt.plot(generation[power][fr:to])
        plt.plot(generation_truth[power][fr:to])
        plt.plot(diff[power][fr:to])
        plt.legend(['sim', 'true', 'diff'])
        plt.show()
    power = 'coal'
    normalized[power][:144].plot()
    normalized_truth[power][:144].plot()
