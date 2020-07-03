import pandas as pd
import numpy as np

if __name__ == '__main__':

    df = pd.read_excel(r'C:\Users\rieke\Desktop\Wind_Neu.xlsx', index_col=0)

    for i in range(1000, 100000, 1000):
        local = df[np.logical_and(df['Plz'] <= i + 1000, df['Plz'] > i)]



